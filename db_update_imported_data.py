import time
from config import LOCAL_DB_CREDENTIALS
from db.db_client import PsqlClient, establish_psql_connection
from db.db_add_vaccine_mentions import update_vaccine_mentions


def main():
    update_imported_data()


def update_imported_data():
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    client = PsqlClient(conn)

    unchecked_vaccine_tweet_ids = client.get_unchecked_vaccine_tweet_ids()
    update_vaccine_mentions(client, unchecked_vaccine_tweet_ids)
    # TODO: reorganize the update shit so it isn't scattered over all these different files.
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


#    cur = conn.cursor()
#    cur.execute('''UPDATE links SET full_url_trimmed = LEFT(full_url,1000)
#               WHERE full_url_trimmed IS NULL''')
#    conn.commit()
#    cur.close()
#    client.create_misinfo_tweets_table()
    

if __name__ == "__main__":
    main()
