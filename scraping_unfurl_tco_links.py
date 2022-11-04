import sqlite3
import re
import sys
import csv
import time
import urllib.request as request
from lxml import html
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import config
from utilities import flatten_obj_list
from db.models import TwitterLink


def main():
    """Reads Links table from database and attempts to unfurl tco_url where full_url isn't available"""
    chunk_size = 10000
    conn = sqlite3.connect(config.DATABASE) # TODO: update to psql db
    c = conn.cursor()

    c.execute('''SELECT id, tweet_id, tco_url FROM links WHERE full_url IS NULL ORDER BY id ASC''')
    # c.execute('''SELECT id, tweet_id, tco_url, full_url FROM links WHERE full_url IS NOT NULL ORDER BY id DESC''')
    results = c.fetchall()

    twitter_links = create_link_objs(results)
    for twitter_link_chunk in yield_chunks(twitter_links, chunk_size):
        t1 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=64) as pool:
            links_with_full_url = list(pool.map(add_full_url_to_twitter_link, twitter_link_chunk))
        # twitter_link.full_url will be None if there was an http error. These are removed before data is added to the db.
        full_urls_and_ids = [[twitter_link.full_url, twitter_link.db_id] for twitter_link in links_with_full_url if
                             twitter_link.full_url]
        query = '''UPDATE links SET full_url = ? WHERE id = ?'''
        c.executemany(query, full_urls_and_ids)
        conn.commit()
        t2 = time.perf_counter()
        time_for_loop = t2-t1
        percent_of_remaining_completed = len(full_urls_and_ids) / len(twitter_links)
        if percent_of_remaining_completed == 0:
            time_estimate_for_remaining = 0
        else:
            time_estimate_for_remaining = int(time_for_loop/percent_of_remaining_completed)
        print(f"{len(full_urls_and_ids)} links scraped. {len(twitter_links)} remaining. "
              f"{time_estimate_for_remaining}s remaining)")
              
        # links with no source accumulate, so as this script is re-run, failure-rate is naturally very high
        # success_rate = len(full_urls_and_ids) / chunk_size
        # failure_rate = 1 - success_rate
        # if failure_rate > 0.1:
            # print(f"quitting due to failure rate ({failure_rate})")
            # sys.exit()


def create_link_objs(results):
    objs = deque()
    for result in results:
        twitter_obj = TwitterLink(tweet_id=result[1],
                                  tco_url=result[2])
        twitter_obj.db_id = result[0]
        objs.append(twitter_obj)
    return objs


def yield_chunks(full_list, chunk_size):
    while full_list:
        chunk = []
        for i in range(chunk_size):
            try:
                chunk.append(full_list.popleft())
            except IndexError:
                break
        yield chunk


def add_full_url_to_twitter_link(twitter_link):
    try:
        twitter_link.full_url = tco_url_to_full_url(twitter_link.tco_url)
    except:
        twitter_link.full_url = None
    return twitter_link


def tco_url_to_full_url(tco_url):
    req = request.Request(tco_url,
                                 headers={
                                     'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'})
    response = request.urlopen(req).read()
    tree = html.fromstring(response)
    response_url = tree.xpath('//title')[0].text
    return response_url


### Other funcs not used in main thing


def extract_links(conn):
    c = conn.cursor()
    c.execute('''SELECT id, tweet_text FROM tweets''')
    results = c.fetchall()
    tweet_ids_and_urls = []
    for result in results:
        result_urls = extract_tco_links(result[1])
        for url in result_urls:
            tweet_ids_and_urls.append([result[0], url])
    c.execute('''CREATE TABLE IF NOT EXISTS links
                     (id INTEGER PRIMARY KEY,
                     tweet_id INTEGER NOT NULL,
                     tco_url TEXT NOT NULL,
                     full_url TEXT)''')
    conn.commit()
    query = f"INSERT INTO links (tweet_id, tco_url) VALUES(?, ?)"
    c.executemany(query, tweet_ids_and_urls)
    conn.commit()


def extract_tco_links(tweet_text):
    pattern = "(https:\/\/t.co\/[A-Za-z0-9]+)"
    links = re.findall(pattern=pattern, string=tweet_text)
    return links


def objs_to_csv(obj_list, output_file_name):
    flat_objs = flatten_obj_list(obj_list)
    with open(output_file_name, 'w', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(flat_objs)


if __name__ == "__main__":
    main()


