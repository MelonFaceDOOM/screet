import sys
import csv
import requests
import pandas as pd
from db_client import PsqlClient, establish_psql_connection
from config import LOCAL_DB_CREDENTIALS
from models import FactCheckerArticle
from psycopg2.extras import execute_values
from requests.exceptions import HTTPError, ConnectionError, ReadTimeout


original_misinfo_file = r'other_data/COVIDGlobal Misinformation Dashboard 2020-22_Page 1_Table.csv'
misinfo_file = r'other_data/misinfo_with_domain.csv'
misinfo_backup_file = r'other_data/misinfo_with_domain_bkp.csv'
domain_counts_file = 'domains_with_counts.csv'


headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Sec-GPC': '1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.79 Safari/537.36'
}

def main():
    pass

def rescrape_articles_with_no_html():
    articles_without_html = get_articles_from_db_with_no_article_text()
    article_ids_and_html = []
    for a in articles_without_html:
        try:
            r = requests.get(a[1], headers=headers, timeout=10, verify=False)
            article_html = r.text
        except (HTTPError, ConnectionError, ReadTimeout) as err:
            article_html = ''
        if len(article_html) > 1000:
            article_ids_and_html.append((a[0], r.text))
            
        if len(article_ids_and_html) == 10:
            update_articles_in_db(article_ids_and_html)
            article_ids_and_html = []
            
    if article_ids_and_html:
        update_articles_in_db(article_ids_and_html)

        
def get_articles_from_db_with_no_article_text():
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    cur = conn.cursor()
    cur.execute('''SELECT * FROM fact_checker_articles''')
    articles_without_html = cur.fetchall()
    articles_without_html = [a for a in articles_without_html if len(a[6])<1000]
    articles_without_html.sort(key=lambda x: x[5])
    cur.close()
    conn.close()
    return articles_without_html


def update_articles_in_db(article_ids_and_html):
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    cur = conn.cursor()
    query = ('''UPDATE fact_checker_articles AS f 
                   SET article_html = c.article_html
                   FROM (VALUES %s)
                   AS c(article_id, article_html)
                   WHERE c.article_id = f.id ''')
    execute_values(cur, query, article_ids_and_html)
    conn.commit()
    cur.close()
    conn.close()


def import_articles_from_misinfo_file_to_db():
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    client = PsqlClient(conn)
    articles = []
    for article in df_to_article_objs_with_html():
        articles.append(article)
        if len(articles) == 10:
            print("saving")
            client.save_many_fact_checker_articles(articles)
            articles = []
    if articles:
        client.save_many_fact_checker_articles(articles)
        
        
def df_to_article_objs_with_html():
    df = pd.read_csv(misinfo_file)
    for index, row in df.iterrows():
        article = row_to_article_obj(row)
        try:
            r = requests.get(article.article_link)
            article.article_html = r.text
        except:
            pass
        yield article


def row_to_article_obj(row):
    article = FactCheckerArticle(review_date=row['Review Date'],
        claim=row['Claim (auto-translated into English)'],
        rating=row['Rating Provided by Fact-checker'],
        article_link=row['Review Article'],
        article_domain=row['Domain'],
        article_source_link=row['source_url'])
    return article


def get_source_link(url):
    domain = url_to_domain(url)
    if domain not in article_element_xpaths:
        raise ValueError(f'no known xpath associated with the domain {domain}')
    article_element_xpath = article_element_xpaths[domain]
    article_tree = get_element_by_xpath_from_page(url, article_element_xpath)
    first_link = get_first_link_in_element(article_tree)
    return first_link
    
    
if __name__ == "__main__":
    main()
