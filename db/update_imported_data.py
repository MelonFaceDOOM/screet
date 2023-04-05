import json
from config import LOCAL_DB_CREDENTIALS
from db.db_client import PsqlClient, establish_psql_connection
from db.models import TwitterVaccineMention


vaccines_file = r"../vaccines/vaccines.txt"


def main():
    update_imported_data()


def update_imported_data():
    check_new_tweets_for_vaccine_mentions()
    check_for_misinfo_links()


def check_for_misinfo_links():
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    cur = conn.cursor()
    cur.execute('''UPDATE links SET full_url_trimmed = LEFT(full_url,1000)
                  WHERE full_url_trimmed IS NULL''')
    conn.commit()
    cur.close()
    client = PsqlClient(conn)
    client.create_misinfo_tweets_table()


def check_new_tweets_for_vaccine_mentions():
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    client = PsqlClient(conn)

    unchecked_vaccine_tweet_ids = client.get_unchecked_vaccine_tweet_ids()
    update_vaccine_mentions(client, unchecked_vaccine_tweet_ids)
    cur = conn.cursor()
    chunk_size = 3_000_000
    while unchecked_vaccine_tweet_ids:
        chunk = unchecked_vaccine_tweet_ids[:chunk_size]
        unchecked_vaccine_tweet_ids = unchecked_vaccine_tweet_ids[chunk_size:]
        cur.execute('''INSERT INTO vaccine_tweets
                       (SELECT tweets.*
                       FROM tweets INNER JOIN vaccine_mentions
                       ON tweets.id = vaccine_mentions.tweet_id
                       AND tweets.id in %s)
                       ON CONFLICT DO NOTHING''', (chunk, ))
        conn.commit()
        client.mark_tweet_ids_as_vaccine_checked(chunk)
    cur.close()


def update_vaccine_mentions(client, tweet_ids):
    vaccines = get_vaccines()
    for vaccine in vaccines:
        for vaccine_synonym in vaccines[vaccine]:
            formatted_query = format_query(vaccine_synonym)
            for results in client.search_phrase_in_tweet_ids(formatted_query, chunk_size=3_000_000,
                                                             tweet_ids=tweet_ids):
                vaccine_mentions = []
                for result in results:
                    vaccine_mention = TwitterVaccineMention(
                        tweet_id=result['id'],
                        vaccine_mentioned=vaccine_synonym)
                    vaccine_mentions.append(vaccine_mention)
                if vaccine_mentions:
                    client.save_many_vaccine_mentions(vaccine_mentions)


def format_query(raw):
    words = raw.split(" ")
    words = [word.split("'")[0] for word in words]
    formatted_query = " ".join(words)
    return formatted_query


def get_vaccines():
    with open(vaccines_file, 'r', encoding='utf-8') as f:
        vaccines = json.loads(f.read())
    return vaccines


if __name__ == "__main__":
    update_imported_data()
