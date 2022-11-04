import csv
from psycopg2.extras import RealDictCursor, execute_values
from misinfo.misinfo_lists import archive_domains
from utilities import extract_domain


def get_src_domains_with_counts(conn):
    cur = conn.cursor()     
    cur.execute('''SELECT article_source_link FROM fact_checker_articles WHERE length(article_html)>1000''')
    results = cur.fetchall()
    domains = [extract_domain(result[0]) for result in results if result[0]]
    unique_domains = set(domains)
    domains_and_counts = []
    for domain in unique_domains:
        domain_count = domains.count(domain)
        domains_and_counts.append((domain, domain_count))
    domains_and_counts = sorted(domains_and_counts, key=lambda x: x[1], reverse=True)
    cur.close()
    return domains_and_counts
    
    
def get_articles_for_src_domain(conn, src_domain):
    cur = conn.cursor(cursor_factory=RealDictCursor)     
    cur.execute('''SELECT * FROM fact_checker_articles WHERE length(article_html)>1000''')
    results = cur.fetchall()
    articles_in_src_domain = []
    for result in results:
        if result['article_source_link']:
            domain = extract_domain(result['article_source_link'])
            if domain == src_domain:
                articles_in_src_domain.append(result)
    cur.close()
    return articles_in_src_domain
    
    
def get_fc_links_for_src_domain(conn, src_domain):
    cur = conn.cursor()     
    cur.execute('''SELECT article_link, article_source_link FROM fact_checker_articles WHERE length(article_html)>1000''')
    results = cur.fetchall()
    fc_links_and_src_domains = [[result[0], extract_domain(result[1])] for result in results if result[1]]
    fc_links_in_specified_src_domain = [i[0] for i in fc_links_and_src_domains if i[1]==src_domain]
    fc_links_in_specified_src_domain = sorted(fc_links_in_specified_src_domain)
    cur.close()
    return fc_links_in_specified_src_domain
    

def get_articles_for_fc_domain_and_src_domain(conn, fc_domain, src_domain):
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute('''SELECT * FROM fact_checker_articles WHERE article_domain = %s AND length(article_html)>1000''', (fc_domain,))
    results = cur.fetchall()
    fc_links_and_src_domains = [result for result in results if extract_domain(result['article_source_link'])==src_domain]
    cur.close()
    return fc_links_and_src_domains
    
    
def get_source_domains_from_fc_domain(conn, fc_domain):
    cur = conn.cursor()
    cur.execute('''SELECT article_source_link FROM fact_checker_articles WHERE article_domain = %s AND length(article_html)>1000''', (fc_domain,))
    results = cur.fetchall()
    links = [result[0] for result in results]
    domains = [extract_domain(link) for link in links if link]
    unique_domains = set(domains)
    domains_and_counts = []
    for domain in unique_domains:
        domain_count = domains.count(domain)
        domains_and_counts.append((domain, domain_count))
    domains_and_counts = sorted(domains_and_counts, key=lambda x: x[1], reverse=True)
    cur.close()
    return domains_and_counts
    

def get_articles_by_domain(conn, domain):
    cur = conn.cursor(cursor_factory = RealDictCursor)
    cur.execute('''SELECT * FROM fact_checker_articles WHERE article_domain = %s AND length(article_html)>1000''', (domain,))
    results = cur.fetchall()
    cur.close()
    return results


def update_article_domain_in_db(conn):
    cur = conn.cursor()
    query = ('''SELECT id, article_domain FROM fact_checker_articles''')
    cur.execute(query)
    results = cur.fetchall()
    article_ids_and_domains = [[result[0], extract_domain(result[1])] for result in results if result[1]]
    query = ('''UPDATE fact_checker_articles AS f 
                   SET article_domain = c.article_domain
                   FROM (VALUES %s)
                   AS c(article_id, article_domain)
                   WHERE c.article_id = f.id ''')
    execute_values(cur, query, article_ids_and_domains)
    conn.commit()
    cur.close()
    
    
def get_archive_articles(conn):
    cur = conn.cursor(cursor_factory=RealDictCursor)     
    cur.execute('''SELECT * FROM fact_checker_articles WHERE length(article_html)>1000''')
    results = cur.fetchall()
    archive_articles = []
    for result in results:
        if result['article_source_link']:
            domain = extract_domain(result['article_source_link'])
            if domain in archive_domains:
                archive_articles.append(result)
    cur.close()
    return archive_articles
    
    
def get_non_archive_articles(conn):
    cur = conn.cursor(cursor_factory=RealDictCursor)     
    cur.execute('''SELECT * FROM fact_checker_articles WHERE length(article_html)>1000''')
    results = cur.fetchall()
    non_archive_articles = []
    for result in results:
        if result['article_source_link']:
            domain = extract_domain(result['article_source_link'])
            if domain not in archive_domains:
                non_archive_articles.append(result)
    cur.close()
    return non_archive_articles
    
    
def get_archive_articles_without_true_source(conn):
    archive_articles = get_archive_articles(conn)
    archive_articles = [article for article in archive_articles if article['true_source_link'] == '']
    return archive_articles


def get_articles_for_id_list(conn, id_list):
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute('''SELECT * FROM fact_checker_articles WHERE id in %s''', (id_list,))
    results = cur.fetchall()
    return results

    
def find_gaps(conn):
    cur = conn.cursor()
    # cur.execute('''SELECT count(*) FROM fact_checker_articles''')
    # total = cur.fetchone()[0]
    # cur.execute('''SELECT count(*) FROM fact_checker_articles WHERE length(article_html)>1000''')
    # with_html = cur.fetchone()[0]
    # cur.execute('''SELECT id, article_link, claim, article_domain FROM fact_checker_articles WHERE length(article_html)>1000 AND length(article_source_link) > 5''')
    # cur.execute('''SELECT count(*) FROM fact_checker_articles WHERE length(article_html)>1000 AND length(article_source_link) > 5''')
    # with_source = cur.fetchone()[0]
    # cur.execute('''SELECT id, article_domain, article_link,article_source_link, true_source_link, true_source_link_trimmed FROM fact_checker_articles WHERE length(article_html)>1000 AND length(article_source_link) > 5 AND length(true_source_link) > 5''')
    cur.execute('''SELECT id, article_domain, article_link, article_source_link, true_source_link, true_source_link_trimmed FROM fact_checker_articles WHERE length(article_html)>1000 AND length(article_source_link) <5''')
    results = cur.fetchall()
    print(results[:10])
    domains = [r[1] for r in results]
    unique_domains = list(set(domains))
    domain_counts = {}
    for domain in unique_domains:
        domain_counts[domain] = domains.count(domain)
    results_with_domain_counts = []
    for r in results:
        new_results = list(r)
        new_results.append(domain_counts[r[1]])
        results_with_domain_counts.append(new_results)
        
    with open("no_source.csv", "w", encoding="utf-8", newline='') as f:
        writer = csv.writer(f)
        writer.writerows(results_with_domain_counts)
