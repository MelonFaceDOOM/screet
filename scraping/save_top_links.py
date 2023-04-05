import pandas as pd
from config import LOCAL_DB_CREDENTIALS
from db.db_client import PsqlClient, establish_psql_connection
from db.models import TwitterVaccineMention
from psycopg2.extras import execute_values, RealDictCursor


def main():
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute('''SELECT links.*, tweets.retweet_count FROM
                  (SELECT * FROM tweets WHERE retweet_count > 100) tweets
                  INNER JOIN
                  (SELECT * FROM links WHERE full_url IS NOT NULL AND domain != %s) links
                  ON links.tweet_id = tweets.id''', ('twitter.com',))
    r = cur.fetchall()
    print(len(r))
    df = pd.DataFrame(r)
    df.to_csv('links_rt100.csv', index=False)
#    print(df['domain'].value_counts())
                  
                  
if __name__ == "__main__":
    main()

