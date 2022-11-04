from config import LOCAL_DB_CREDENTIALS
from db_client import establish_psql_connection


def main():
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    redo_trimming(conn)
    
    
def redo_trimming(conn):
    cur = conn.cursor()
    cur.execute('''DROP INDEX idx_links_full_url_trimmed''')
    cur.execute('''DROP INDEX idx_fact_checker_articles_true_source_link_trimmed''')
    cur.execute('''UPDATE links SET full_url_trimmed = LEFT(full_url,1000)''')
    cur.execute('''UPDATE fact_checker_articles SET true_source_link_trimmed = LEFT(true_source_link,1000)''')
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_links_full_url_trimmed ON links(full_url_trimmed)''')
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_fact_checker_articles_true_source_link_trimmed
                   ON fact_checker_articles(true_source_link_trimmed)''')
    conn.commit()
    cur.close()


def trim_full_url(conn):
    """deprecated by redo_trimming()"""
    cur = conn.cursor(cursor_factory = RealDictCursor)
    while True:
        t1 = time.perf_counter()
        cur.execute('''SELECT * FROM links WHERE length(full_url) > 5 AND full_url_trimmed IS NULL LIMIT 1000000''')
        result = cur.fetchall()
        t2 = time.perf_counter()
        article_ids_and_full_urls_trimmed = []
        for r in result:
            full_url_trimmed = r['full_url'].split('?')[0]
            article_ids_and_full_urls_trimmed.append([r['id'], full_url_trimmed])
        t3 = time.perf_counter()
        query = ('''UPDATE links AS l 
                    SET full_url_trimmed = c.full_url_trimmed
                    FROM (VALUES %s)
                    AS c(article_id, full_url_trimmed)
                    WHERE c.article_id = l.id ''')
        execute_values(cur, query, article_ids_and_full_urls_trimmed)
        conn.commit()
        t4 = time.perf_counter()
        print(t2-t1, t3-t2, t4-t2)
        if len(result) < 1000000:
            break
    cur.close()
    
            
def trim_true_source_link(conn):
    """deprecated by redo_trimming()"""
    cur = conn.cursor(cursor_factory = RealDictCursor)  
    cur.execute('''SELECT * FROM fact_checker_articles WHERE true_source_link IS NOT NULL''')
    results = cur.fetchall()
    article_ids_and_source_links_trimmed = []
    for r in results:
        true_source_link_trimmed = r['true_source_link'].split('?')[0]
        article_ids_and_source_links_trimmed.append([r['id'], true_source_link_trimmed])
    query = ('''UPDATE fact_checker_articles AS f
                SET true_source_link_trimmed = c.true_source_link_trimmed
                FROM (VALUES %s)
                AS c(article_id, true_source_link_trimmed)
                WHERE c.article_id = f.id ''')
    execute_values(cur, query, article_ids_and_source_links_trimmed)
    conn.commit()
    cur.close()
    
    
def remove_lengthy_urls(conn):
    """deprecated by redo_trimming()"""
    cur = conn.cursor(cursor_factory = RealDictCursor)
    cur.execute('''UPDATE links
                   SET full_url_trimmed = NULL
                   WHERE length(full_url_trimmed)>1000''')
    cur.execute('''SELECT count(*) FROM links WHERE length(full_url_trimmed)>5''')
    # result = cur.fetchone()
    # print(result)
    # cur.execute('''SELECT count(*) FROM links WHERE length(full_url_trimmed)>1000''')
    # result = cur.fetchone()
    # print(result)
    conn.commit()
    cur.close()
    
    
if __name__ == "__main__":
    main()
