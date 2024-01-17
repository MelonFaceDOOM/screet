import json
import psycopg2
import io
import csv
import time
from psycopg2.extras import execute_values, RealDictCursor
from utilities import clean_csv_value


def establish_psql_connection(database, user, password, host, port):
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    return conn


def generate_tweets_table_creation_sql(table_name, temp_table=False):
    return f"""CREATE {"TEMPORARY " if temp_table else ""}TABLE IF NOT EXISTS {table_name}
     (id BIGINT PRIMARY KEY NOT NULL,
     date_entered TIMESTAMP DEFAULT current_timestamp,
     source TEXT NOT NULL,
     user_id BIGINT NOT NULL,
     conversation_id BIGINT NOT NULL,
     created_at DATE NOT NULL,
     tweet_text TEXT NOT NULL,
     ts TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', replace(tweet_text, '''',''))) STORED,
     vaccine_tweet_checked BOOLEAN NOT NULL DEFAULT false,
     retweet_count INTEGER,
     like_count INTEGER,
     reply_count INTEGER,
     quote_count INTEGER)"""


class PsqlClient:
    def __init__(self, conn):
        self.conn = conn
        
    def make_core_tables(self):
        cur = self.conn.cursor()
        cur.execute(generate_tweets_table_creation_sql(table_name="tweets"))
        cur.execute('''CREATE TABLE IF NOT EXISTS users
                         (id BIGINT PRIMARY KEY NOT NULL,
                         date_entered TIMESTAMP DEFAULT current_timestamp,
                         created_at DATE NOT NULL,
                         followers_count INTEGER NOT NULL,
                         following_count INTEGER NOT NULL,
                         tweet_count INTEGER NOT NULL,
                         verified BOOLEAN NOT NULL,
                         location TEXT)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS errors
                         (tweet_id BIGINT PRIMARY KEY NOT NULL,
                         title TEXT NOT NULL,
                         detail TEXT,
                         error_type TEXT,
                         message TEXT)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS links
                         (id SERIAL PRIMARY KEY,
                         tweet_id BIGINT NOT NULL,
                         tco_url TEXT NOT NULL,
                         full_url TEXT,
                         full_url_trimmed TEXT,
                         UNIQUE (tweet_id, tco_url))''')
        cur.execute('''CREATE TABLE IF NOT EXISTS vaccine_mentions
                         (id SERIAL PRIMARY KEY,
                         tweet_id BIGINT NOT NULL,
                         vaccine_mentioned TEXT NOT NULL,
                         UNIQUE (tweet_id, vaccine_mentioned))''')
        cur.execute('''CREATE TABLE IF NOT EXISTS fact_checker_articles
                         (id SERIAL PRIMARY KEY,
                         article_link TEXT NOT NULL UNIQUE,
                         review_date DATE NOT NULL,
                         claim TEXT NOT NULL,
                         rating TEXT NOT NULL,
                         article_domain TEXT NOT NULL,
                         article_html TEXT,
                         article_source_link TEXT,
                         true_source_link TEXT,
                         true_source_link_trimmed TEXT)''')
        self.conn.commit()
        cur.close()
        
    def make_indices(self):
        cur = self.conn.cursor()
        cur.execute('''CREATE INDEX IF NOT EXISTS idx_tweets_date ON tweets(created_at)''')
        cur.execute('''CREATE INDEX IF NOT EXISTS ts_idx ON tweets USING GIN (ts);''')
        cur.execute('''CREATE INDEX IF NOT EXISTS idx_links_full_url_trimmed ON links(full_url_trimmed)''')
        cur.execute('''CREATE INDEX IF NOT EXISTS idx_fact_checker_articles_true_source_link_trimmed
                         ON fact_checker_articles(true_source_link_trimmed)''')
        cur.execute('''CREATE INDEX IF NOT EXISTS idx_links_tweet_id ON links(tweet_id)''')
        self.conn.commit()
        cur.close()
    
    def save_many_objects_to_table(self, objs, table_name):
        cur = self.conn.cursor()
        cols = objs[0].attrs()
        cols_string = ", ".join(cols)
        query = f"INSERT INTO {table_name} ({cols_string}) VALUES %s ON CONFLICT DO NOTHING"
        values = [[getattr(obj, col) for col in cols] for obj in objs]
        execute_values(cur, query, values)
        self.conn.commit()
        cur.close()

    def save_object(self, obj, table_name):
        cur = self.conn.cursor()
        cols = obj.attrs()
        cols_string = ", ".join(cols)
        values = ', '.join(['%({})s'.format(col) for col in cols])
        insert = 'INSERT INTO {0} ({1}) VALUES ({2}) ON CONFLICT DO NOTHING'.format(table_name, cols_string, values)
        obj_as_dict = {col: getattr(obj, col) for col in cols}
        query = cur.mogrify(insert, obj_as_dict)
        cur.execute(query)
        self.conn.commit()
        cur.close()

    def save_user(self, user):
        self.save_object(user, "users")

    def save_many_tweets(self, tweets):
        self.save_many_objects_to_table(tweets, "tweets")

    def save_many_users(self, users):
        self.save_many_objects_to_table(users, "users")

    def save_many_errors(self, errors):
        self.save_many_objects_to_table(errors, "errors")

    def save_many_links(self, links):
        self.save_many_objects_to_table(links, "links")

    def save_many_vaccine_mentions(self, vaccine_mentions):
        self.save_many_objects_to_table(vaccine_mentions, "vaccine_mentions")

    def save_many_fact_checker_articles(self, fact_checker_articles):
        self.save_many_objects_to_table(fact_checker_articles, "fact_checker_articles")
        
    def save_parsed_response_data(self, parsed_response_data):
        t1 = time.perf_counter()
        if parsed_response_data.tweets:
            self.save_many_tweets(parsed_response_data.tweets)
        t2 = time.perf_counter()
        if parsed_response_data.users:
            try:
                self.save_many_users(parsed_response_data.users)
            except:
                for user in parsed_response_data.users:
                    try:
                        self.save_user(user)
                    except:
                        cols = user.attrs()
                        print([getattr(user, col) for col in cols])  # certain names caused issues. i think it's fixed
        t3 = time.perf_counter()
        if parsed_response_data.errors_with_id:
            self.save_many_errors(parsed_response_data.errors_with_id)
        t4 = time.perf_counter()
        if parsed_response_data.links:
            self.save_many_links(parsed_response_data.links)
        t5 = time.perf_counter()
        # print("tweets: ", t2-t1)
        # print("users: ", t3-t2)
        # print("errors: ", t4-t3)
        # print("links: ", t5-t4)
        
    def tweet_id_to_text(self, tweet_id):
        tweet_id = int(tweet_id)
        cur = self.conn.cursor()
        query = '''SELECT tweet_text
                        FROM tweets
                        WHERE id = %s'''
        cur.execute(query, (tweet_id,))
        tweet_text = cur.fetchall()
        if tweet_text:
            tweet_text = tweet_text[0]
        cur.close()
        return tweet_text

    def search_words_in_tweets(self, search_words):
        search_string = " ".join(search_words)
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(f'''SELECT *
                        FROM tweets
                        WHERE ts @@ plainto_tsquery('english', '{search_string}');''')
        results = cur.fetchall()
        cur.close()
        return results
        
    def search_phrase_in_tweets(self, search_phrase, chunk_size):
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(f'''SELECT *
                        FROM tweets
                        WHERE ts @@ phraseto_tsquery('english', '{search_phrase}')
                        ORDER BY id ASC
                        LIMIT %s;''',(chunk_size,))
        results = cur.fetchall()
        yield results
        if len(results) == chunk_size:
            while len(results) > 0:
                max_id = results[-1]['id']
                cur.execute(f'''SELECT *
                                FROM tweets
                                WHERE ts @@ phraseto_tsquery('english', '{search_phrase}')
                                AND id > %s
                                ORDER BY id ASC
                                LIMIT %s;''',(max_id, chunk_size))
                results = cur.fetchall()
                yield results
        cur.close()

    def search_phrase_in_tweet_ids(self, search_phrase, chunk_size, tweet_ids):
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        while tweet_ids:
            chunk = tweet_ids[:chunk_size]
            tweet_ids = tweet_ids[chunk_size:]
            cur.execute(f'''SELECT *
                            FROM tweets
                            WHERE ts @@ phraseto_tsquery('english', '{search_phrase}')
                            AND id in %s''', (chunk,))
            results = cur.fetchall()
            yield results
        cur.close()
        
    def create_vaccine_tweets_table(self):
        cur = self.conn.cursor()
        cur.execute('''DROP TABLE IF EXISTS vaccine_tweets''')
        cur.execute('''CREATE TABLE vaccine_tweets AS
                       SELECT DISTINCT tweets.*
                       FROM tweets INNER JOIN vaccine_mentions
                       ON tweets.id = vaccine_mentions.tweet_id''')
        cur.execute('''CREATE INDEX vaccine_ts_idx ON vaccine_tweets USING GIN (ts);''')
        cur.execute('''ALTER TABLE vaccine_tweets ADD UNIQUE (id)''')
        self.conn.commit()
        cur.close()
        
    def mark_tweet_ids_as_vaccine_checked(self, tweet_ids):
        cur = self.conn.cursor()
        cur.execute('''UPDATE tweets set vaccine_tweet_checked=true
                       WHERE id in %s''', (tweet_ids,))
        self.conn.commit()

    def get_unchecked_vaccine_tweet_ids(self):
        cur = self.conn.cursor()
        cur.execute('''SELECT id FROM tweets WHERE vaccine_tweet_checked=false''')
        results = cur.fetchall()
        unchecked_vaccine_tweet_ids = tuple([i[0] for i in results])
        return unchecked_vaccine_tweet_ids
        
    def clear_vaccine_mentions_table(self):
        cur = self.conn.cursor()
        cur.execute('''TRUNCATE vaccine_mentions''')
        self.conn.commit()
        cur.close()
        
    def get_vaccine_related_tweets(self):
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('''SELECT * FROM vaccine_tweets''')
        results = cur.fetchall()
        cur.close()
        return results
    
    def search_vaccine_tweet_text(self, search_phrase, data_source):
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(f'''SELECT * FROM vaccine_tweets
                        WHERE ts @@ phraseto_tsquery('english', '{search_phrase}')
                        AND source = %s''', (data_source,))
        results = cur.fetchall()
        cur.close()
        return results

    def query_vaccine_tweet_text(self, query, data_source):
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(f'''SELECT * FROM vaccine_tweets
                        WHERE ts @@ %s
                        AND source = %s''', (query, data_source))
        results = cur.fetchall()
        cur.close()
        return results

    def create_misinfo_tweets_table(self):
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('''DROP TABLE IF EXISTS misinfo_tweets''')
        cur.execute('''CREATE TABLE misinfo_tweets AS
                       SELECT tb1.*, tweets.created_at, tweets.retweet_count
                       FROM (
                           SELECT links.*, fact.article_link, fact.article_source_link, fact.claim,
                               fact.rating, fact.true_source_link_trimmed
                           FROM (
                               SELECT * FROM links WHERE length(full_url_trimmed)>5
                           ) links
                           INNER JOIN (
                               SELECT * FROM fact_checker_articles WHERE id in (
                                   SELECT max(id) FROM fact_checker_articles WHERE length(true_source_link_trimmed)>5
                                   GROUP BY true_source_link_trimmed
                               )
                           ) fact
                           ON
                               links.full_url_trimmed = fact.true_source_link_trimmed
                       ) tb1
                       INNER JOIN (
                           SELECT * FROM tweets
                       ) tweets
                       ON tb1.tweet_id = tweets.id
                       ''')
        self.conn.commit()
        cur.close()
            
    def get_all_misinfo_tweets(self):
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('''SELECT * FROM misinfo_tweets''')
        results = cur.fetchall()
        self.conn.commit()
        cur.close()
        return results
        
    def search_misinfo_tweets_claim_text(self, search_phrase):
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(f'''SELECT * FROM misinfo_tweets
                        WHERE to_tsvector('english', claim) @@ to_tsquery('english', '{search_phrase}');''')
        results = cur.fetchall()
        cur.close()
        return results

    def create_query(self, phrase):
        cur = self.conn.cursor()
        cur.execute('''SELECT phraseto_tsquery(%s)''', (phrase,))
        r = cur.fetchall()
        cur.close()
        return r[0][0]

    def debug_query(self, phrase):
        cur = self.conn.cursor()
        cur.execute('''SELECT ts_debug(%s)''', (phrase,))
        r = cur.fetchall()
        r = [row[0] for row in r]
        debug = "\n".join(r)
        cur.close()
        return debug