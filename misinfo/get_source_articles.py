import re
from lxml import html
from selenium import webdriver
import time
from psycopg2.extras import execute_values
from misinfo.browse_fact_checker_articles import get_articles_by_domain,  get_non_archive_articles
from misinfo.misinfo_lists import blacklist, whitelist_priority, archive_domains, archive_links_that_are_not_misinfo
from utilities import extract_domain, minimal_url

archive_links_that_are_not_misinfo = [minimal_url(link) for link in archive_links_that_are_not_misinfo]

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
    pass


def create_firefox_driver():
    options = webdriver.FirefoxOptions()
    options.page_load_strategy = 'eager'  # stops loading as soon as basic html is loaded. saves a massive amount of time for this kind of thing
    driver = webdriver.Firefox(executable_path=r"C:\proj\covid_scraping\geckodriver.exe", options=options)
    return driver


def extract_and_save_source_link_in_all_articles(conn):
    articles = []
    for domain in article_element_xpaths:
        articles += get_articles_by_domain(conn, domain)
    extract_and_save_source_link_for_articles(conn, articles)
    
    
def extract_and_save_source_link_for_articles(conn, articles):
    non_archive_set = []  # will contain non_archive links
    archive_rescrape_set = []  # will contain new archive links and empty TSL, since they will need to be separately scraped
    for article in articles:
        new_source_link = extract_source_link_for_article(article)
        old_source_link = article['article_source_link']
        if new_source_link:
            if extract_domain(new_source_link) not in archive_domains:
                non_archive_set.append((article['id'], new_source_link, new_source_link))
            elif old_source_link and minimal_url(old_source_link) != minimal_url(new_source_link):
                archive_rescrape_set.append((article['id'], new_source_link, ''))
    update_articles_in_db(conn, non_archive_set+archive_rescrape_set)


def extract_source_link_for_article(article):
    article_links = extract_all_links_from_article(article)
    source_link = None
    if article_links:
        source_link = choose_source_link_from_all_article_links(article['article_domain'], article_links)
    return source_link
    

def extract_all_links_from_article(article):
    tree = html.fromstring(article['article_html'])
    article_xpath = article_element_xpaths[article['article_domain']]
    article_elements = tree.xpath(article_xpath)
    article_links = []
    for element in article_elements:
        links = element.xpath('.//a')
        for link in links:
            try:
                article_links.append(link.get('href'))
            except:
                pass
    return article_links
    
    
def choose_source_link_from_all_article_links(article_domain, article_links):
    domain_blacklist = blacklist + [article_domain]
    eligible_article_links = []
    for link in article_links:
        if link:
            link_domain = extract_domain(link)
            if not link_domain:
                link_domain = article_domain
            if link_domain not in domain_blacklist and minimal_url(link) not in archive_links_that_are_not_misinfo:
                eligible_article_links.append(link)
    if not eligible_article_links:
        return None
    for whitelist_domain in whitelist_priority:
        for link in eligible_article_links:
            if whitelist_domain == extract_domain(link):
                return link
    return eligible_article_links[0]  # return first eligible link if no whitelist domains were found
    
    
def update_articles_in_db(conn, id_sl_tsl):
    """takes a list lists with id, source_link and true_source_link and updates
    them in the db"""
    cur = conn.cursor()
    query = ('''UPDATE fact_checker_articles AS f 
                   SET article_source_link = c.article_source_link,
                   true_source_link = c.true_source_link
                   FROM (VALUES %s)
                   AS c(article_id, article_source_link, true_source_link)
                   WHERE c.article_id = f.id ''')
    execute_values(cur, query, id_sl_tsl)
    conn.commit()
    cur.close()

    
def get_source_from_archive_is(page_source):
    tree = html.fromstring(page_source)
    page_not_found = tree.xpath('//h1[contains(text(), "Not Found")]')
    if page_not_found:
        return "unretrievable"
    save_from_box = tree.xpath('//input[@type="text"]')[0]
    return save_from_box.get('value')
    
    
def get_source_from_archive_org(url):
    # print(page_source)
    pattern = "https?:\/\/web.archive.org\/(?:(?:web(?:\/\d+)?)|(?:save))\/(.+)"
    match = re.search(pattern, url)
    return match.group(1)
    
        
def get_source_from_perma_cc(page_source):
    tree = html.fromstring(page_source)
    live_page_content = tree.xpath('//div[@class="col col-sm-2 _livepage"]')[0]
    live_page_link = live_page_content.xpath('.//a')[0]
    source_link = live_page_link.get('href')
    return source_link
    

def get_source_from_many_archive_urls(source_links):
    '''allows many archive urls to be scraped within one selenium session'''
    archive_domain_extraction_funcs = {
        'perma.cc': get_source_from_perma_cc,
        'archive.is': get_source_from_archive_is,
        'archive.md': get_source_from_archive_is,
        'archive.ph': get_source_from_archive_is,
        'archive.vn': get_source_from_archive_is,
        'archive.fo': get_source_from_archive_is
    }
    driver = create_firefox_driver()
    driver.set_page_load_timeout(30)
    for source_link in source_links:
        print(source_link)
        domain = extract_domain(source_link)
        if domain == 'web.archive.org':
            true_source_link = get_source_from_archive_org(source_link)
        else:
            driver.get(source_link)        
            extraction_func = archive_domain_extraction_funcs[domain]
            try:
                true_source_link = extraction_func(page_source=driver.page_source)
            except Exception as e:
                driver.quit()
                raise Exception(e)
        yield source_link, true_source_link
        
            
def get_and_save_source_for_archive_links(conn, archive_urls):
    previous_remaining_count = 0
    sleep_timer = 60
    max_consecutive_failures = 6
    consecutive_failures = 0
    while True:
        completed_archive_urls = []
        remaining_count = len(archive_urls)
        if remaining_count == 0:
            print('All archive articles scraped.')
            break
        if remaining_count == previous_remaining_count and consecutive_failures == max_consecutive_failures:
            print(f'Quitting since last {consecutive_failures} loops scraped 0 articles.')
            break
        else:
            previous_remaining_count = remaining_count
        try:
            for source_link, true_source_link in get_source_from_many_archive_urls(archive_urls):
                add_true_source_to_article(conn, source_link, true_source_link)
                completed_archive_urls.append(source_link)
                sleep_timer = 60 # reset on successful scrape
                consecutive_failures = 0 
        except Exception as e:
            consecutive_failures += 1
            print(e)
            print(f'failed {consecutive_failures} times consecutively')
            print(f'sleeping for {sleep_timer}')
            time.sleep(sleep_timer)
            sleep_timer = 2 * sleep_timer
        for url in completed_archive_urls:
            archive_urls.remove(url)


def update_true_source_for_non_archive_articles(conn):
    non_archive_articles = get_non_archive_articles(conn)
    article_ids_and_true_source_link = [[article['id'], article['article_source_link']] for article in non_archive_articles]
    cur = conn.cursor()
    query = ('''UPDATE fact_checker_articles AS f 
                   SET true_source_link = c.true_source_link
                   FROM (VALUES %s)
                   AS c(article_id, true_source_link)
                   WHERE c.article_id = f.id ''')
    execute_values(cur, query, article_ids_and_true_source_link)
    conn.commit()
    cur.close()
    
        
def trim_fact_checker_links(conn):
    # commented out code would also redo links table
    cur = conn.cursor()
    # cur.execute('''DROP INDEX idx_links_full_url_trimmed''')
    cur.execute('''DROP INDEX idx_fact_checker_articles_true_source_link_trimmed''')
    # cur.execute('''UPDATE links SET full_url_trimmed = LEFT(full_url,1000)''')
    cur.execute('''UPDATE fact_checker_articles SET true_source_link_trimmed = LEFT(true_source_link,1000)''')
    # cur.execute('''CREATE INDEX IF NOT EXISTS idx_links_full_url_trimmed ON links(full_url_trimmed)''')
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_fact_checker_articles_true_source_link_trimmed
                   ON fact_checker_articles(true_source_link_trimmed)''')
    conn.commit()
    cur.close()
   

def add_true_source_to_article(conn, source_link, true_source_link):
    cur = conn.cursor()
    cur.execute('''UPDATE fact_checker_articles SET true_source_link = %s WHERE article_source_link = %s''', 
                (true_source_link, source_link))
    conn.commit()
    cur.close()


def check_source_for_one_link(perma_cc_link):
    driver = create_firefox_driver()
    driver.set_page_load_timeout(30)
    driver.get('perma_cc_link')
    page_source = driver.page_source
    return get_source_from_perma_cc(page_source)


if __name__ == "__main__":
    main()

