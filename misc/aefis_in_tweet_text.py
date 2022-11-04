from db_client import PsqlClient, establish_psql_connection
from config import LOCAL_DB_CREDENTIALS
from psycopg2.extras import execute_values, RealDictCursor
from dataclasses import dataclass
from typing import get_type_hints
import json


# this is a mishmash of a bunch of things because i needed to quickly find twees that mention both vaccine and symptoms within a text file containing only tweet text

def main():

   

    # conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    # cur = conn.cursor()
    # cur.execute('''CREATE TABLE IF NOT EXISTS vaccine_tweets_from_file
                   # (tweet_text TEXT NOT NULL,
                    # ts tsvector GENERATED ALWAYS AS (to_tsvector('english', tweet_text)) STORED)''')
    # conn.commit()
    # with open('full_text_links_removed.csv', 'r', encoding='utf-8') as f:
        # data = f.readlines()
    # data = [[d] for d in data]
    # query = f"INSERT INTO vaccine_tweets_from_file(tweet_text) VALUES(%s)"
    # cur.executemany(query, data)
    # # cur.execute('''INSERT INTO vaccine_tweets_from_file (tweet_text) VALUES %s''', data)
    
    # cur.execute('''CREATE INDEX IF NOT EXISTS ts_temp_idx ON vaccine_tweets_from_file USING GIN (ts);''')
    
    # cur.execute('''CREATE TABLE IF NOT EXISTS vaccine_mentions_from_file
                 # (id SERIAL PRIMARY KEY,
                 # tweet_text TEXT NOT NULL,
                 # vaccine_mentioned TEXT NOT NULL)''')
    # conn.commit()
    # cur.close()
    # vaccines = get_vaccines()
    # vaccine_mentions = []
    # for vaccine in vaccines:
        # for vaccine_synonym in vaccines[vaccine]:
            # formatted_query = format_query(vaccine_synonym)
            # results = search_table(formatted_query, 'vaccine_tweets_from_file', conn)
            # for result in results:
                # vaccine_mention = TwitterVaccineMention(
                    # tweet_text = result['tweet_text'],
                    # vaccine_mentioned=vaccine_synonym)
                # vaccine_mentions.append(vaccine_mention)
    # save_many_vaccine_mentions(vaccine_mentions, conn)

    # tweet_text_from_tweets_with_vaccine_and_symptom_mention = get_tweet_text_with_symptoms(conn)
    
    # with open('aefis_2022_08_20.txt', 'w', encoding='utf-8') as f:
        # out = [t.replace('\n', '/n') for t in tweet_text_from_tweets_with_vaccine_and_symptom_mention]
        # out = "\n".join(tweet_text_from_tweets_with_vaccine_and_symptom_mention)
        # f.write(out)
        
        
     with open('aefis_2022_08_20.txt', 'r', encoding='utf-8') as f, open('aefis_2022_08_20_no_duplicates.txt', 'w', encoding='utf-8') as f_out:
        seen = set()
        for line in f:
            if line not in seen: 
                seen.add(line)
                f_out.write(line)

    
def format_query(raw):
    words = raw.split(" ")
    words = [word.split("'")[0] for word in words]
    formatted_query = " ".join(words)
    return formatted_query
    
def search_table(search_phrase, tablename, conn):
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(f'''SELECT * FROM vaccine_tweets_from_file
                    WHERE ts @@ phraseto_tsquery('english', '{search_phrase}');''')
    results = cur.fetchall()
    cur.close()
    return results
    
def save_many_objects_to_table(objs, table_name, conn):
    cur = conn.cursor()
    cols = objs[0].attrs()
    cols_string = ", ".join(cols)
    query = f"INSERT INTO {table_name} ({cols_string}) VALUES %s ON CONFLICT DO NOTHING"
    values = [[getattr(obj, col) for col in cols] for obj in objs]
    execute_values(cur, query, values)
    conn.commit()
    cur.close()


def save_many_vaccine_mentions(vaccine_mentions, conn):
    save_many_objects_to_table(vaccine_mentions, "vaccine_mentions_from_file", conn)
    
    
def get_vaccines():
    vaccines_file = r"other_data\vaccines.txt"
    with open(vaccines_file, 'r', encoding='utf-8') as f:
        vaccines = json.loads(f.read())
    return vaccines
    
    
def get_symptoms():
    symptom_file = "other_data\COVID-Twitter-Symptom-Lexicon.tsv"
    aefis_file = r"other_data\aefis.txt"
    with open(aefis_file, 'r', encoding='utf-8') as f:
        symptoms = json.loads(f.read())
    return symptoms
    

def get_tweet_text_with_symptoms(conn):
    symptoms = get_symptoms()
    tweet_text = []
    for symptom in symptoms:
        for symptom_synonym in symptoms[symptom]:
            formatted_query = format_query(symptom_synonym)
            results = search_table(formatted_query, 'vaccine_mentions_from_file', conn)
            text = [r['tweet_text'] for r in results]
            tweet_text += text
    return tweet_text

    
@dataclass
class FlattenableDataClass:
    @classmethod
    def attrs(cls):
        return [attr for attr in get_type_hints(cls)]

    
@dataclass
class TwitterVaccineMention(FlattenableDataClass):
    tweet_text: str
    vaccine_mentioned: str

    def __repr__(self):
        return f"TwitterVaccineMention <{self.tweet_text}>: {self.vaccine_mentioned}"


if __name__ == "__main__":
    main()

# cur.execute()
# cur.execute('''DROP TABLE IF EXISTS vaccine_tweets''')
# 

# cur.execute('''CREATE INDEX vaccine_ts_idx ON vaccine_tweets USING GIN (ts);''')
# self.conn.commit()
# cur.close()
