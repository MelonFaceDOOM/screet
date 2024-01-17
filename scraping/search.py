import re
import os
import datetime
import json
from config import BEARER_TOKEN
from utilities import get_file_paths_from_folder
from scraping.twitter_api_requestor import SearchManager
from scraping.parse_twitter_response import parse_twitter_response
import sys

# Should we re-search days that have already been searched?
# No.
# Results from Oct5 search: 14324
# Results from Oct7 search: 12823
# Results from Oct7 search on or before Oct 5: 9633
# Results from Oct7 search on or before Oct 5 that are in Oct5 search: 9620


output_folder = "D:/screet_data/search_results/not imported"
imported_folder = "D:/screet_data/search_results/imported to db"


with open('scraping/search_config.txt', 'r') as f:
    search_config = json.load(f)


def main():
    execute_search_config()


def execute_search_config():
    for search_title in search_config:
        search_output_folder, search_imported_folder = make_search_folders(search_title)
        search_terms = search_config[search_title]
        for search_term in search_terms:
            skip_search_term = False
            previous_results_file_paths =\
                get_all_file_paths_for_search_term_results(search_term, [search_output_folder, search_imported_folder])
            if not previous_results_file_paths:
                output_file_path = os.path.join(search_output_folder, f"{search_term}_0.txt")
                since_id = None
            else:
                most_recent_file_path = get_file_path_with_biggest_number(previous_results_file_paths)
                since_id, days_since_last_search = get_since_id_from_file(most_recent_file_path)
                progress_file = locate_progress_file(most_recent_file_path)
                if progress_file:
                    output_file_path = most_recent_file_path
                else:
                    output_file_name = get_new_output_filename(most_recent_file_path)
                    output_file_path = os.path.join(search_output_folder, output_file_name)
                    if days_since_last_search < 1:
                         # todo: this is returning -1 for searches that took place on the same day
                        # only skip if there last search was within 1 day and there is no progress file
                        skip_search_term = True
            if skip_search_term:
                print(f'skipping {search_term} because most recent search was less than 1 day ago.')
            else:
                search_for_term(search_term, output_file_path, since_id)


def make_search_folders(search_title):
    output = os.path.join(output_folder, search_title)
    if not os.path.exists(output):
        os.makedirs(output)
    imported = os.path.join(imported_folder, search_title)
    if not os.path.exists(imported):
        os.makedirs(imported)
    return output, imported


def get_all_file_paths_for_search_term_results(search_term, folders=None):
    if folders is None:
        folders = []
    file_paths = []
    for folder in folders:
        file_paths += get_file_paths_from_folder(folder)
    results_file_paths = []
    for file_path in file_paths:
        filename = re.split(r'[/\\]', file_path)[-1]
        pattern = f'({search_term})_(\d+)\.txt'
        match = re.match(pattern, filename)
        if match:
            results_file_paths.append(file_path)
    return results_file_paths


def get_since_id_from_file(file_path):
    tweet = get_newest_tweet_from_file(file_path)
    difference = datetime.datetime.now() - tweet.created_at
    print(f"it has been {difference} days since the last scraped tweet was created")
    if difference.days > 7:
        since_id = None
    else:
        since_id = str(tweet.id)
    return since_id, difference.days
    
    
def get_file_path_with_biggest_number(results_file_paths):
    biggest_file_number = -1
    file_path_with_biggest_number = ""
    for file_path in results_file_paths:
        pattern = '.*_(\d+)\.txt'
        match = re.match(pattern, file_path)
        file_number = int(match.group(1))
        if file_number > biggest_file_number:
            biggest_file_number = file_number
            file_path_with_biggest_number = file_path
    return file_path_with_biggest_number


def get_newest_tweet_from_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = f.read()
    first_line = data.split('\n')[0]
    parsed_response_data = parse_twitter_response(first_line)
    highest_id = 0
    newest_tweet = None
    for tweet in parsed_response_data.tweets:
        if tweet.id > highest_id:
            highest_id = tweet.id
            newest_tweet = tweet
    return newest_tweet


def get_new_output_filename(previous_file_path):
    previous_filename = re.split(r'[/\\]', previous_file_path)[-1]
    pattern = '(.*)_(\d+)\.txt'
    match = re.match(pattern, previous_filename)
    previous_file_number = int(match.group(2))
    new_file_number = previous_file_number + 1
    new_filename = f'{match.group(1)}_{new_file_number}.txt'
    return new_filename


def locate_progress_file(data_file_path):
    data_directory, data_file_name = os.path.split(data_file_path)
    _ = data_file_name.split(".")
    _ = ".".join(_[:-1]) if len(_) > 1 else _[0]
    progress_file_name = _ + "_progress.txt"
    progress_file_path = os.path.join(data_directory, progress_file_name)
    if os.path.isfile(progress_file_path):
        return progress_file_path
    else:
        return None


def search_for_term(search_term, output_file_path, since_id=None):
    print(f"starting search for {search_term}\n"
          f"saving to {output_file_path}\n"
          f"using since_id: {since_id}")
    searcher = SearchManager(search_terms=[search_term],
                             output_file=output_file_path,
                             bearer_token=BEARER_TOKEN)
    searcher.start_new_search(since_id=since_id)


if __name__ == "__main__":
    main()

