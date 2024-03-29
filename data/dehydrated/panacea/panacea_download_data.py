import subprocess
import datetime
import gzip
import requests
from lxml import html
import re
import os
from utilities import get_file_names_and_paths_from_folder


def main():
    download_and_process_panacea_ids()


def download_and_process_panacea_ids():
    panacea_source_folder = "D:/work/projects/panacea"
    panacea_processed_folder = "D:/work/projects/screet/data/dehydrated/panacea/ids/ids_2022"
    download_panacea_update(panacea_source_folder) # does git pull to update panacea folder
    files_to_process = get_downloaded_files_to_process(panacea_source_folder, panacea_processed_folder)
    for filepath, file_date in files_to_process:
        tsv_data = get_tsv_data_from_source_file(filepath)
        ids = get_ids_from_file(tsv_data)
        output_filename = f"{file_date.date()}_clean-dataset.txt"
        output_filepath = os.path.join(panacea_processed_folder, output_filename)
        with open(output_filepath, 'w') as f:
            f.write('\n'.join(ids))


def download_panacea_update(panacea_source_folder):
    command = ['git', 'pull']
    process = subprocess.Popen(command, stdout=subprocess.PIPE, cwd=panacea_source_folder)
    output, unused_err = process.communicate()
    print(output)


def get_downloaded_files_to_process(panacea_source_folder, panacea_processed_folder):
    processed_files = [name for name, path in get_file_names_and_paths_from_folder(panacea_processed_folder)]
    processed_dates = [datetime.datetime.strptime(filename[:10], '%Y-%m-%d') for filename in processed_files]
    latest_processed_date = max(processed_dates)
    source_files_and_paths = get_file_names_and_paths_from_folder(panacea_source_folder)
    files_to_process = []
    date_filename_pattern = "\d{4}-\d{2}-\d{2}_clean-dataset.tsv(?:.gz)?"
    for filename, filepath in source_files_and_paths:
        if re.match(date_filename_pattern, filename):
            file_date = datetime.datetime.strptime(filename[:10], '%Y-%m-%d')
            if file_date > latest_processed_date:
                files_to_process.append([filepath, file_date])
    return files_to_process


def get_tsv_data_from_source_file(source_file):
    if source_file[-2:] == "gz":
        with gzip.open(source_file, mode='rt', encoding='cp850') as f:
            tsv_data = f.read()
    else:
        with open(source_file, encoding='cp850') as f:
            tsv_data = f.read()
    return tsv_data


def get_ids_from_file(tsv_file):
    lines = tsv_file.split('\n')
    tweet_ids = []
    for line in lines[1:]:
        line = line.split('\t')
        tweet_ids.append(line[0])
    return tweet_ids


def get_date_from_url(url):
    match = re.search(pattern="(\d{4}-\d{2}-\d{2})", string=url)
    date = None
    if match:
        date = datetime.date.fromisoformat(match.group(1))
    return date
    
    
def convert_to_dl_url_type_1(relative_url):
    relative_url = relative_url.replace('tree', 'raw')
    date_string = get_date_from_url(relative_url).isoformat()
    filename = f"{date_string}_clean-dataset.tsv.gz"
    download_url = "https://github.com" + relative_url + "/" + filename
    return download_url


def convert_to_dl_url_type_2(relative_url):
    relative_url = relative_url.replace('tree', 'raw')
    date_string = get_date_from_url(relative_url).isoformat()
    filename = f"{date_string}_clean-dataset.tsv"
    download_url = "https://github.com" + relative_url + "/" + filename
    return download_url
    
    
def download_file(url, save_folder):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        save_location = save_folder + "/" + url.split('/')[-1]
        with open(save_location, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
    return save_location


def old_shit():
    start_date = datetime.date.fromisoformat("2022-11-27")
    save_folder = "source/source_6"
    daily_files_page = requests.get("https://github.com/thepanacealab/covid19_twitter/tree/master/dailies")
    tree = html.fromstring(daily_files_page.text)
    daily_file_urls = tree.xpath("//div[@role='row']/div/span/a")
    daily_file_urls = [element.get('href') for element in daily_file_urls]
    urls_to_scrape = []
    for url in daily_file_urls:
        date = get_date_from_url(url)
        if date and date >= start_date:
            urls_to_scrape.append(url)
    for url in urls_to_scrape:
        try:
            download_url = convert_to_dl_url_type_1(url)
            download_file(download_url, save_folder=save_folder)
        except:
            download_url = convert_to_dl_url_type_2(url)
            download_file(download_url, save_folder=save_folder)


if __name__ == "__main__":
    main()
