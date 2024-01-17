import json
from config import LOCAL_DB_CREDENTIALS
from db.db_client import PsqlClient, establish_psql_connection
from psycopg2.extras import RealDictCursor
import sys
import pandas as pd

conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
client = PsqlClient(conn)


def main():
    # with open()
    i = 0
    # for r in yield_vaccine_tweets(conn, 5000000):
    #     if r:
    #         print(len(r))
    #         df = pd.DataFrame(r)
    #         print(len(df))
    #         df.to_csv(f'covid_vaccine_tweets_{i}_1.csv', index=False)
    #         i += 1
    #     sys.exit()
    df = pd.read_csv('covid_vaccine_tweets_0_1.csv')
    print(len(df))


    # files = ['vaccine_tweets_0.csv', 'covid_vaccine_tweets_0.csv', 'covid_vaccine_tweets_1.csv', 'covid_vaccine_tweets_2.csv', 'covid_vaccine_tweets_3.csv']
    # for filename in files:
    #     df = pd.read_csv(filename)
    #     df = df[['id', 'created_at', 'tweet_text', 'source']]
    #     df.to_csv(filename)


def yield_vaccine_tweets(conn, chunk_size):
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(f'''SELECT *
                    FROM vaccine_tweets
                    WHERE source = %s
                    ORDER BY id ASC
                    LIMIT %s;''', ('panacea', chunk_size))
    results = cur.fetchall()
    yield results
    while len(results) > 0:
        max_id = results[-1]['id']
        cur.execute(f'''SELECT *
                        FROM vaccine_tweets
                        WHERE source = %s
                        AND id > %s
                        ORDER BY id ASC
                        LIMIT %s;''', ('panacea', max_id, chunk_size))
        results = cur.fetchall()
        yield results
    cur.close()


    # cur = conn.cursor()
    # cur.execute(f'''SELECT COUNT(*) FROM vaccine_tweets''')
    # r = cur.fetchall()
    # print(r)

    # cur.execute('''SELECT id from tweets
    #                ORDER BY id ASC
    #                LIMIT 100000''')
    # r = cur.fetchall()
    # print(r[-1])

# 1k = 1241577033227960321
# 10k = 1241588957198143489
# 100k = 1241695342292328449


def search_tweets_for_new_vaccine_list(conn):
    cur = conn.cursor()
    max_id = 1241588957198143489
    with open('vaccines/vaccine_products.txt', 'r', encoding='utf-8') as f:
        data = f.read()
        vaccines_dict = json.loads(data)
        vaccines = []
        for key in vaccines_dict:
            vaccines += vaccines_dict[key]
    cur.execute(f'''SELECT id
                    FROM tweets
                    WHERE ts @@ phraseto_tsquery('english', 'pfizer')''')
    r = cur.fetchall()
    print(len(r))
    # cur.execute(f'''SELECT id
    #                 FROM tweets
    #                 WHERE ts @@ phraseto_tsquery('english', 'covid')
    #                 AND id < %s
    #                 ORDER BY id ASC''', (max_id,))
    # r = cur.fetchall()
    # print(len(r))


if __name__ == "__main__":
    main()
