import json
from config import LOCAL_DB_CREDENTIALS
from db.db_client import PsqlClient, establish_psql_connection
from db.models import TwitterVaccineMention

vaccines_file = r"vaccines/vaccines.txt"


def main():
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    client = PsqlClient(conn)
    update_vaccine_mentions(client)


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
    main()
