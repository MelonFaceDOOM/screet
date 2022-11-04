import pandas as pd
from psycopg2.extras import execute_values, RealDictCursor
from db.db_client import PsqlClient, establish_psql_connection
from config import LOCAL_DB_CREDENTIALS
from utilities import extract_domain
import datetime

domains = (
    "israelnationalnews.com",
    "oann.com",
    "thefederalist.com",
    "dailyexpose.co.uk",
    "expose-news.com",
    "theexpose.uk",
    "dailyexpose.uk",
    "rumble.com",
    "off-guardian.org",
    "thegatewaypundit.com",
    "justthenews.com",
    "americasfrontlinedoctors.org",
    "pieceofmindful.com",
    "greatgameindia.com",
    "zerohedge.com",
    "childrenshealthdefense.org",
    "rairfoundation.com",
    "nationalfile.com",
    "swprs.org",
    "judicialwatch.org",
    "aaronsiri.substack.com",
    "amgreatness.com",
    "tobyrogers.substack.com",
    "vicgeorge.net",
    "conservativereview.com",
    "vaccineimpact.com",
    "renz-law.com",
    "realrawnews.com",
    "healthandmoneynews.wordpress.com",
    "lifesitenews.com",
    "jdfor2020.com",
    "Realfarmacy.com",
    "globalresearch.ca",
    "collective-evolution.com",
    "jedanews.com",
    "ripostelaique.com",
    "mercola.com",
    "lesmoutonsrebelles.com",
    "sonsoflibertymedia.com",
    "wakingtimes.com",
    "nowtheendbegins.com"
)

def main():
    # conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    # client = PsqlClient(conn)
    
    df = pd.read_csv('misinfo_links.csv', encoding='utf-8')
    df['created_at'] = pd.to_datetime(df['created_at'])
    # get_month_counts(df)
    
    df = df[['domain', 'like_count']]
    df = df.groupby(['domain']).sum()
    df = df.sort_values(by=['like_count'], ascending=False)
    print(df.head(15))

def export_misinfo_articles_to_csv(conn):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        sql = cur.mogrify('''SELECT tweets.created_at, tweets.tweet_text, tweets.source, tweets.like_count, tweets.retweet_count, links.*
                       FROM links JOIN tweets
                       ON links.tweet_id = tweets.id
                       WHERE links.domain IN %s;''', (domains,))
        cur.execute(sql)
        r = cur.fetchall()
        df = pd.DataFrame(r)
        df.to_csv('misinfo_links.csv', index=False, encoding='utf-8')
       


def add_domain_to_links(conn):
    with conn.cursor() as cur:
        cur.execute('''ALTER TABLE links ADD COLUMN domain TEXT''')
        cur.execute('''SELECT id, full_url FROM LINKS''')
        results = cur.fetchall()
        print(len(results))
        id_domain = []
        for r in results:
            domain = extract_domain(r[1])
            if domain:
                id_domain.append([r[0], domain])
        
        update_query = """UPDATE links AS l 
                  SET domain = e.domain 
                  FROM (VALUES %s) AS e(id, domain) 
                  WHERE e.id = l.id;"""

        execute_values(
            cur, update_query, id_domain, template=None, page_size=100
        )
        conn.commit()

        
def get_month_counts(df):
    df = df[['domain', 'like_count', 'created_at']]
    df['created_at'] = df.apply(lambda x: convert_to_month(x['created_at']), axis=1)
    df = df.groupby(['domain', 'created_at']).sum()
    df = df.sort_values(by=['like_count'], ascending=False)
    print(df.head(15))
    
    
def convert_to_month(date):
    # date = date.replace(day=1)
    date = datetime.date(year=date.year, month=date.month, day=1)
    return date
    
if __name__ == "__main__":
    main()
