import click
import datetime
import snscrape.modules.twitter as sntwitter
import psycopg2
from psycopg2 import errors
from psycopg2.extras import execute_values, DictCursor
from config import username, password, host, port, dbname
import os

UniqueViolation = errors.lookup('23505')

SEARCH_TERMS = ["covid"]
LIMIT = os.environ.get("SCREET_LIMIT", 300000)
LIMIT = int(LIMIT)
SINCE = os.environ.get("SCREET_SINCE", "2021-01-31")
UNTIL = os.environ.get("SCREET_UNTIL", "2021-02-01")
SINCE = datetime.datetime.strptime(SINCE, "%Y-%m-%d")
UNTIL = datetime.datetime.strptime(UNTIL, "%Y-%m-%d")


@click.group()
def main():
    pass

@main.command()
def scrape():
    conn = establish_connection()
    make_table(conn)
    col_names = get_col_names(conn, "tweets")
    oldest_tweet = get_tweets(conn, order="ASC", number_of_tweets=1, min_date=SINCE, max_date=UNTIL)
    if oldest_tweet:
        oldest_tweet = oldest_tweet[0]
        end_date = oldest_tweet['date'] + datetime.timedelta(days=1)
        
        start_dbcount = get_dbcount()
        begin_scraping(conn=conn, col_names=col_names, max_id=oldest_tweet['id'], until=end_date)
        end_dbcount = get_dbcount()
        if start_dbcount == end_dbcount:
            # band-aid for an issue. Twitter returns tweets starting with the end of the day.
            # this means max-id can be used to pick up where scraping left off mid day
            # however, once a day is completed, max-id will prevent the next day from being accessed.
            # therefore, scraping should be re-initiated with no max-id
            begin_scraping(conn=conn, col_names=col_names, until=end_date)
    else:
        begin_scraping(conn=conn, col_names=col_names)
    conn.close()


@main.command()
@click.option('-c', '--confirmation', is_flag=True, help="confirm 'yes' to clear database.")
def clear(confirmation):
    conn = establish_connection()
    cur = conn.cursor()
    reply = 'x'
    while reply[0] not in ['y', 'n']:
        reply = input("Are you sure you want to clear the database? (y/n): ").lower().strip()
    if reply[0] == 'y':
        cur.execute('''DROP TABLE tweets''')
        conn.commit()
    cur.close()
    conn.close()


@main.command()
def dbcount():
    print(get_dbcount())
    
    
@main.command()
def monthly():
    month_counts = get_count_by_month()
    print(month_counts)
    
    
@main.command()
def daily():
    day_counts = get_count_by_day()
    print(day_counts)
    
    
def get_count_by_month():
    conn = establish_connection()
    cur = conn.cursor(cursor_factory=DictCursor)
    cur.execute(f'''SELECT date_trunc('month', date) AS month, count(id) as monthly_count
                    FROM tweets
                    GROUP BY month''')
    month_counts = cur.fetchall()
    cur.close()
    conn.close()
    return month_counts


def get_count_by_day():
    conn = establish_connection()
    cur = conn.cursor(cursor_factory=DictCursor)
    cur.execute(f'''SELECT date_trunc('day', date) AS day, count(id) as daily_count
                    FROM tweets
                    GROUP BY day''')
    day_counts = cur.fetchall()
    cur.close()
    conn.close()
    return day_counts
    

def get_dbcount():
    conn = establish_connection()
    cur = conn.cursor(cursor_factory=DictCursor)
    cur.execute(f'''SELECT count(*) FROM tweets''')
    tweet_count = cur.fetchone()
    if tweet_count:
        tweet_count = tweet_count[0]
    else:
        tweet_count = 0
    cur.close()
    conn.close()
    return tweet_count


def establish_connection():
    conn = psycopg2.connect(database=dbname, user=username, password=password, host=host, port=port)
    return conn


def make_table(conn):
    cur = conn.cursor()
    #cur.execute('''DROP TABLE tweets''')
    cur.execute('''CREATE TABLE IF NOT EXISTS tweets
                     (id BIGINT PRIMARY KEY,
                     date_entered TIMESTAMP DEFAULT current_timestamp,
                     conversationId BIGINT NOT NULL,
                     date TIMESTAMP NOT NULL,
                     renderedContent TEXT NOT NULL,
                     coordinates TEXT,
                     inReplyToTweetId BIGINT,
                     inReplyToUser BIGINT,
                     lang TEXT,
                     likeCount INTEGER,
                     media TEXT,
                     retweetedTweet TEXT,
                     quotedTweet TEXT,
                     place TEXT, 
                     quoteCount INTEGER,
                     retweetCount INTEGER,
                     url TEXT NOT NULL,
                     user_id BIGINT NOT NULL)''')
    conn.commit()
    cur.close()


def get_col_names(conn, table_name):
    cur = conn.cursor()
    cur.execute(f"Select * FROM {table_name} LIMIT 0")
    col_names = [desc[0].lower() for desc in cur.description]  # make everything lowercase
    cur.close()
    return col_names


def get_tweets(conn, order="DESC", number_of_tweets=1, min_date=None, max_date=None):
    cur = conn.cursor(cursor_factory=DictCursor)
    if min_date and max_date:
        cur.execute(f'''SELECT * FROM tweets 
                       WHERE date BETWEEN %s and %s
                       ORDER BY date {order}''', (min_date, max_date))
    else:
        cur.execute(f'''SELECT * FROM tweets ORDER BY date {order}''')
    tweets = cur.fetchmany(number_of_tweets)
    cur.close()
    return tweets


def begin_scraping(conn, col_names, search_terms=None, limit=LIMIT, since=SINCE, until=UNTIL, max_id=None):
    if search_terms is None:
        search_terms = SEARCH_TERMS

    search_terms_string = " OR ".join(search_terms) # i.e. "covid OR corona OR pandemic"
    keywords_string = "since:" + since.strftime("%Y-%m-%d") + " until:" + until.strftime("%Y-%m-%d")
    # i.e. "since:2021-01-01 until:2021-12-01"
    if max_id:
        keywords_string += " max_id:" + str(max_id)
    query = search_terms_string + " " + keywords_string
    print(query)

    for i, tweet in enumerate(
            sntwitter.TwitterSearchScraper(query=query).get_items()):
        tweet = clean_tweet(tweet, col_names)
        save_tweet(conn, tweet=tweet)
        if i >= limit:
            break


def clean_tweet(tweet, col_names):
    tweet = tweet.__dict__
    tweet['user_id'] = tweet['user'].id  # must rename to corresponding DB column (psql seems not to accept 'user' as a column'),
                                           # and save url string rather than User obj
    if tweet['inReplyToUser']:
        tweet['inReplyToUser'] = tweet['inReplyToUser'].id
    if tweet['retweetedTweet']:
        tweet['retweetedTweet'] = tweet['retweetedTweet'].url
    if tweet['quotedTweet']:
        tweet['quotedTweet'] = tweet['quotedTweet'].url
    if tweet['media']:
        media_urls = []
        for media in tweet['media']:
            if media.__class__.__name__ in ['Gif', 'Video']:
                url = media.thumbnailUrl
            elif media.__class__.__name__ == "Photo":
                url = media.fullUrl
            else:
                url = "tweet media type not recognized"
            media_urls.append(url)
        media_urls = ", ".join([media_url for media_url in media_urls])
        tweet['media'] = media_urls
    if tweet['coordinates']:
        tweet['coordinates'] = str(tweet['coordinates'].latitude) + ", " + str(tweet['coordinates'].longitude)
    if tweet['place']:
        tweet['place'] = tweet['place'].fullName

    keys_to_delete = []
    for key in tweet.keys():
        if key.lower() not in col_names:
            keys_to_delete.append(key)
    for key in keys_to_delete:
        del tweet[key]

    for key in tweet.keys():
        if not tweet[key]:
            tweet[key] = None
    return tweet


def save_tweet(conn, tweet):
    cur = conn.cursor()
    try:
        keys = tweet.keys()
        columns = ','.join(keys)
        values = ','.join(['%({})s'.format(k) for k in keys])
        insert = 'insert into tweets ({0}) values ({1})'.format(columns, values)
        query = cur.mogrify(insert, tweet)
        cur.execute(query)
        conn.commit()
    except UniqueViolation as err:
        cur.execute("ROLLBACK")
    cur.close()


def save_many_tweets(conn, tweets):
    cur = conn.cursor()
    cols = tweets[0].keys()
    query = "INSERT INTO tweets ({}) VALUES %s".format(','.join(cols))
    values = [[value for value in tweet.values()] for tweet in tweets]
    execute_values(cur, query, values)
    conn.commit()
    cur.close()


if __name__ == "__main__":
    main()
