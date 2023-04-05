import csv
import pandas as pd
import requests
import pickle
import urllib.request as request
from concurrent.futures import ThreadPoolExecutor


WORKERS = 16
CHUNK_SIZE = 32
N_LINKS_TO_SCRAPE = 100000000


def main():
    already_scraped_urls = get_already_scraped_urls('scraped_data')
    to_scrape_df = pd.read_csv('links_rt100.csv')
    to_scrape_df = to_scrape_df[~to_scrape_df['full_url'].isin(already_scraped_urls)]
    for scraped_data in scrape_df_in_chunks(to_scrape_df):
        for data_dict in scraped_data:
            append_object_to_file('scraped_data', data_dict)


def get_already_scraped_urls(file_location):
    already_scraped_urls = []
    with open(file_location, 'rb') as f:
        while True:
            try:
                data_dict = pickle.load(f)
                already_scraped_urls.append(data_dict['full_url'])
            except EOFError:
                break
    return already_scraped_urls
    
    
def parse_scraped_data(file_location):
    list_of_dicts = []
    with open(file_location, 'rb') as f:
        while True:
            try:
                list_of_dicts.append(pickle.load(f))
            except EOFError:
                break
    return list_of_dicts
                

def append_object_to_file(file_location, object_to_save):
    with open(file_location, 'ab+') as f:
        p = pickle.dumps(object_to_save, protocol=pickle.HIGHEST_PROTOCOL)
        f.write(p)


def scrape_df_in_chunks(df):
    df = df.sort_values(by=['retweet_count'], ascending=False)
    df = df.iloc[:N_LINKS_TO_SCRAPE]
    for chunk in yield_dict_chunks_from_df(df, CHUNK_SIZE):
        print("attempting chunk")
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            scraped_data = list(pool.map(scrape_link, chunk))
        yield scraped_data
    

def yield_dict_chunks_from_df(df, chunk_size):
    for pos in range(0, len(df), chunk_size):
        sub_df = df[pos:pos + chunk_size]
        yield sub_df.to_dict('records')
        

def scrape_link(link_dict):
    """even when using the timeout arg with the requests library, some requests would still hang when running them concurrently through ThreadPoolExecutor. For whatever reason, urllib.request doesn't have this issue."""
    try:
        req = request.Request(link_dict['full_url'], headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:68.0) Gecko/20100101 Firefox/68.0'})
        
        response = request.urlopen(req, timeout=5)
        link_dict['source_url'] = response.url
        link_dict['html'] = response.read()
    except:
        link_dict['source_url'] = "error"
        link_dict['html'] = "error"
    return link_dict
    

def save_dicts_to_csv(dict_list, output_file_name):
    """this was used when i was saving dicts to a csv file. I switched to a pickled dict list when it got too big."""
    with open(output_file_name, 'a+', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=dict_list[0].keys(), escapechar="\\")
        if f.tell() == 0:
            writer.writeheader()
        writer.writerows(dict_list)

    
def csv_to_pickle(csv_file, pickle_file):
    """this converts a csv to a pickled dict list"""
    with open(csv_file, 'r', encoding='utf-8', newline='') as f:
        f = (row.replace('\0', '') for row in f)
        reader = csv.DictReader(f,  escapechar="\\")
        for row in reader:  
            append_object_to_file(pickle_file, row)


if __name__ == "__main__":
    main()


