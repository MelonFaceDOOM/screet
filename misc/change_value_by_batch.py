import time
from psycopg2.extras import RealDictCursor
from db.db_client import establish_psql_connection
from config import LOCAL_DB_CREDENTIALS


def checked_to_false():
    """Change all the values of one column in batches"""
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    # cur.execute('''ALTER TABLE tweets ADD COLUMN vaccine_tweet_checked BOOLEAN NOT NULL DEFAULT false''')
    chunk_size = 3_000_000
    max_id = 0
    results = ["null"]

    while results:
        t1 = time.perf_counter()
        cur.execute('''SELECT id FROM tweets
                       WHERE id > %s
                       AND vaccine_tweet_checked=false
                       ORDER BY id ASC
                       LIMIT %s''', (max_id, chunk_size))
        results = cur.fetchall()
        if results:
            tweet_ids = tuple([i['id'] for i in results])
            min_id = tweet_ids[0]
            max_id = tweet_ids[-1]
            # cur.execute('''UPDATE tweets SET vaccine_tweet_checked=true
            #                WHERE id in %s''',(tweet_ids,))
            cur.execute('''UPDATE tweets SET vaccine_tweet_checked=true
                           WHERE id >= %s
                           AND id <= %s''', (min_id, max_id))
            t2 = time.perf_counter()
            conn.commit()
            print(t2-t1)
