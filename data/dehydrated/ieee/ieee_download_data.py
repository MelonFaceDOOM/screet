import re
from lxml import html


def main():
"""JS is used to load links on the web page. The easiest way to access these links is to
    manually log in, save the html page, and then run this code on the locally saved html file"""
    with open('ieee_logged_in.html') as f:
        tree = html.fromstring(f.read())
    links = tree.xpath('//span[@class="file"]/a')
    for link in links:
        url = link.attrib['href']
        #0-600 are downloaded manually in larger packs.
        if get_data_set_number(url) and get_data_set_number(url) > 600:
            download_file(url)
            
            
def get_data_set_number(dataset_url):
    match = re.search('corona_tweets_(\d+)', dataset_url)
    if match:
        return int(match.group(1))
    else:
        return None
    
    
def get_filename(dataset_url):
    match = re.search('(corona_tweets_\d+.zip)', dataset_url)
    if match:
        return match.group(1)
    else:
        return None
        
        
def download_file(url):
    local_filename = get_filename(url)
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
    return local_filename
    
    
if __name__ == "__main__":
    main()