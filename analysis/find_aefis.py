import json
import re
import csv
import time
import pandas as pd
from datetime import datetime
from config import LOCAL_DB_CREDENTIALS
from db.db_client import establish_psql_connection, PsqlClient
from psycopg2.extras import RealDictCursor


symptoms_file = r"symptoms/symptom_queries.txt"
data_sources = ["polio", "panacea", "monkeypox"]


def main():
#    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
#    cur = conn.cursor()
#    cur.execute('''SELECT * FROM ts_debug('english','can''t')''')
#    r = cur.fetchall()
#    print(r)

#    client = PsqlClient(conn)
#    r = client.search_vaccine_tweet_text("can't breathe", "polio")
#    print(len(r), r[:3])
    save_symptoms_for_all_data_sources()


def save_symptoms_for_all_data_sources():
    for data_source in data_sources:
        save_all_symptom_tweets_for_data_sources(data_source)


def save_all_symptom_tweets_to_file():
    tweets = get_all_tweets_with_symptoms_with_date_for_data_source('panacea')
    with open('s4.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['symptom', 'tweet_text', 'date'])
        for row in tweets:
            writer.writerow(row)
            
            
def save_all_symptom_tweets_for_data_sources(data_sources):
    for data_source in data_sources:
        all_text_and_date = get_all_tweets_with_symptoms_with_date_for_data_source(data_source)
        unique_text_with_date = clean_and_keep_unique_text_with_date(all_text_and_date)
        
        output_file = f"{data_source}_{datetime.today().strftime('%Y-%m-%d')}.csv"
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['tweet_text', 'date'])
            for row in unique_text_with_date:
                writer.writerow(row)
    
    
def clean_and_keep_unique_text_with_date(all_text_and_date):
    '''cleans text and keeps unique (date is not considered when defining unique)'''
    unique_text_with_date = []
    unique_text = set()
    for tweet_text, tweet_date, symptom in all_text_and_date:
        cleaned_tweet_text = clean_tweet_text(tweet_text)
        if len(cleaned_tweet_text)>3:
            if cleaned_tweet_text not in unique_text:
                unique_text.add(cleaned_tweet_text)
                unique_text_with_date.append([symptom, cleaned_tweet_text, tweet_date])
    unique_text_with_date = list(unique_text_with_date)
    unique_text_with_date.sort(key=lambda x: x[1])
    return unique_text_with_date
    

def clean_and_keep_unique_text_and_date(all_text_and_date):
    """cleans text and keeps unique text/date combos"""
    unique_text_and_date = set()
    for tweet_text, tweet_date in all_text_and_date:
        cleaned_tweet_text = clean_tweet_text(tweet_text)
        if len(cleaned_tweet_text) > 3:
            unique_text_and_date.add((cleaned_tweet_text, tweet_date))
    unique_text_and_date = list(unique_text_and_date)
    unique_text_and_date.sort(key=lambda x: x[1])
    return unique_text_and_date
    
    
def get_symptom_queries():
    with open(symptoms_file, 'r', encoding='utf-8') as f:
        symptoms = f.read().split('\n')
    return symptoms
    
    
def format_query(raw):
    """this deals with the issue of the search not accepting apostrophes.
       i.e. this func will transform can't -> can t
       can will match positively for both can and can't in source tweet text.
       better solution would be to remove apostrophes from the source, and add cant as
       a word to the dict, then replace this func with one that removes apostrophes """
    return raw.replace("'", " ")
#    return f"%{raw}%" # this was for ilike (% is wildcard that lets text appear before and after)
    
    
def get_all_tweets_with_symptoms_with_date_for_data_source(data_source):
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    symptom_queries = get_symptom_queries()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    results = []
    for query in symptom_queries:
        cur.execute(f'''SELECT * FROM vaccine_tweets
                        WHERE ts @@ %s
                        AND source = %s''', (query, data_source))
        r = cur.fetchall()
        for i in r:
            i['query'] = query
        results += r
    tweet_text_and_date = [[r['tweet_text'], r['created_at'], r['query']] for r in results]
    return tweet_text_and_date
    
    
def clean_tweet_text(tweet_text):
    tweet_text = tweet_text.replace(u'\xa0', u' ')
    tweet_text = tweet_text.replace('\n', '/n')
    tweet_text = tweet_text.replace('\r', '/r')
    tweet_text = re.sub("(https:\/\/t.co\/[\w]+)", "", tweet_text)  # remove links
    tweet_text = re.sub("(@[A-z_0-9]+)", "", tweet_text)  # remove mentions
    tweet_text = tweet_text.strip()
    return tweet_text
    
    
def get_all_unique_aefis_with_date_for_data_source(data_source):
    all_text_and_date = get_all_tweets_with_symptoms_with_date_for_data_source(data_source)
    unique_text_with_date = clean_and_keep_unique_text_with_date(all_text_and_date)
    return unique_text_with_date


def save_sv_tweets_between_dates():
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    data_source = "monkeypox"
    sql = f'''SELECT * FROM tweets WHERE created_at >= '2022-04-21'::date
               AND created_at <= '2023-01-10'::date
               AND source = %s'''
    cur.execute(sql, (data_source,))

    tweets = cur.fetchall()
    df = pd.DataFrame(tweets)
    df.to_csv('mpox tweets.csv')

    sql = f'''SELECT * FROM vaccine_tweets WHERE created_at >= '2022-04-21'::date
               AND created_at <= '2023-01-10'::date
               AND source = %s'''
    cur.execute(sql, (data_source,))

    tweets = cur.fetchall()
    df = pd.DataFrame(tweets)
    df.to_csv('mpox vaccine tweets.csv')


if __name__ == "__main__":
    main()

