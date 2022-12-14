import datetime
import requests
from lxml import html
import re 


def main():
    start_date = datetime.date.fromisoformat("2022-09-11")
    save_folder = "source/source_5"
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


if __name__ == "__main__":
    main()
