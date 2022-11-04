from db_client import establish_psql_connection
from config import LOCAL_DB_CREDENTIALS
from misinfo.browse_fact_checker_articles import get_archive_articles
from misinfo.get_source_articles import extract_and_save_source_link_in_all_articles, get_and_save_source_for_archive_links,\
    trim_fact_checker_links

article_element_xpaths = {
    'fullfact.org': '//div[@class="cms-content"]',
    'factcheck.afp.com': '//div[@class="article-entry clearfix"]',
    'factual.afp.com': '//div[@class="article-entry clearfix"]',
    'factuel.afp.com': '//div[@class="article-entry clearfix"]',
    'boomlive.in': '//div[@class="story"]',
    'snopes.com': '//div[@class="single-body card-body rich-text"]',
    'leadstories.com': '//div[@class="mod-full-article-content rte-container"]',
    'usatoday.com': '//div[@class="gnt_ar_b"]',
    'politifact.com': '//article[@class="m-textblock"]',
    'vishvasnews.com': '//div[@class="view-less"]',
    'healthfeedback.org': '//div[@class="entry-content"]',
    'aap.com.au': '//div[@class="c-article__content e-content c-article__content--factcheck"]',
    'factcheck.org': '//div[@class="entry-content"]/div[@id="dpsp-content-top"][1]/following-sibling::p',
    'checkyourfact.com': '//div[@id="ob-read-more-selector"]'
}


def main():
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)

    extract_and_save_source_link_in_all_articles(conn)
    archive_articles = get_archive_articles(conn)
    rescrape_urls = [article['article_source_link'] for article in archive_articles
                     if article['true_source_link'] == '']
    get_and_save_source_for_archive_links(conn, rescrape_urls)
    trim_fact_checker_links(conn)


if __name__ == "__main__":
    main()
