import json
import re
import csv

from config import LOCAL_DB_CREDENTIALS
from db.db_client import establish_psql_connection, PsqlClient


aefis_file = r"symptoms\aefis.txt"
symptom_file = "other_data\COVID-Twitter-Symptom-Lexicon.tsv"

def main():
    data_source = "panacea"
    output_file = "aefi_tweet_text_2022-10-14.txt" 
    unique_text_and_date = get_all_unique_aefis_with_date_for_data_source(data_source)
    unique_text = "\n".join([u[0] for u in unique_text_and_date])
    with open(output_file, "w", newline="", encoding='utf-8') as f:
        f.write(unique_text)
        # writer = csv.writer(f)
        # writer.writerows(unique_text)

def get_all_unique_aefis_with_date_for_data_source(data_source):
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    client = PsqlClient(conn)
    all_text_and_date = get_tweet_text_and_date_with_symptoms(client, data_source)
    unique_text_and_date = clean_and_keep_unique_text_and_date(all_text_and_date)
    return unique_text_and_date
    
def get_tweet_text_and_date_with_symptoms(client, data_source):
    symptoms = get_symptoms()
    all_text_and_date = []
    for symptom in symptoms:
        for symptom_synonym in symptoms[symptom]:
            formatted_query = format_query(symptom_synonym)
            results = client.search_vaccine_tweet_text(formatted_query, data_source=data_source)
            tweet_text_and_date = [[r['tweet_text'], r['created_at']] for r in results]
            all_text_and_date += tweet_text_and_date
    return all_text_and_date
    
    
def get_symptoms():
    with open(aefis_file, 'r', encoding='utf-8') as f:
        symptoms = json.loads(f.read())
    return symptoms
    

def format_query(raw):
    """this deals with the issue of the search not accepting apostrophes.
       i.e. this func will transform can't -> can t
       can will match positively for both can and can't in source tweet text.
       better solution would be to remove apostrophes from the source, and add cant as
       a word to the dict, then replace this func with one that removes apostrophes """
    words = raw.split(" ")
    words = [word.split("'")[0] for word in words]
    formatted_query = " ".join(words)
    return formatted_query


def clean_tweet_text(tweet_text):
    tweet_text = re.sub("(https:\/\/t.co\/[\w]+)", "", tweet_text)  # remove links
    tweet_text = re.sub("(@[A-z_0-9]+)", "", tweet_text)  # remove mentions
    tweet_text = tweet_text.strip()
    tweet_text = tweet_text.replace('\n','/n')
    return tweet_text

def clean_and_keep_unique_text_and_date(all_text_and_date):
    # cleans text and keeps unique (date is not considered when defining unique)
    cleaned_text_and_date = []
    unique_text = set()
    for tweet_text, tweet_date in all_text_and_date:
        cleaned_tweet_text = clean_tweet_text(tweet_text)
        if len(cleaned_tweet_text)>3:
            if cleaned_tweet_text not in unique_text:
                unique_text.add(cleaned_tweet_text)
                cleaned_text_and_date.append([cleaned_tweet_text, tweet_date])
    return cleaned_text_and_date


def get_all_text(client, data_source, output_file):
    # some old shit
    all_tweet_text = get_tweet_text_with_symptoms(client, data_source)
    all_tweet_text = list(set(all_tweet_text))
    tweets_cleaned = []
    for att in all_tweet_text:
        att = re.sub("(https:\/\/t.co\/[\w]+)", "", att)
        att = att.strip()
        att = att.replace('\n','/n')
        if len(att)>3:
            tweets_cleaned.append(att)
    tweets_cleaned = list(set(tweets_cleaned))
    tweets_cleaned = "\n".join(tweets_cleaned)
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(tweets_cleaned)
        

def get_tweet_text_with_symptoms(client, data_source):
    # some old shit
    symptoms = get_symptoms()
    all_tweet_text = []
    for symptom in symptoms:
        for symptom_synonym in symptoms[symptom]:
            formatted_query = format_query(symptom_synonym)
            results = client.search_vaccine_tweet_text(formatted_query, data_source=data_source)
            tweet_text = [str(r['tweet_text']) for r in results]
            all_tweet_text += tweet_text
    return all_tweet_text
    
    
if __name__ == "__main__":
    main()
