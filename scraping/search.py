import re
import os
import datetime

from config import BEARER_TOKEN
from utilities import get_file_names_and_paths_from_folder
from scraping.twitter_api_requestor import SearchManager
from scraping.parse_twitter_response import parse_twitter_response


# Should we re-search days that have already been searched?
# No.
# Results from Oct5 search: 14324
# Results from Oct7 search: 12823
# Results from Oct7 search on or before Oct 5: 9633
# Results from Oct7 search on or before Oct 5 that are in Oct5 search: 9620


output_folder_path = "/data/screet_data/search_results"
already_imported_data = "/data/screet_data/search_results/imported to db"
results_imported = get_file_names_and_paths_from_folder(already_imported_data)
results_not_imported = get_file_names_and_paths_from_folder(output_folder_path)


def main():
    search_term = "monkeypox"
    search_for_term(search_term)
    
    
def search_for_term(search_term):
    # previous_file_path = get_previous_file(search_term)
    # since_id = get_since_id_from_previous_file(previous_file_path)
#    since_id = None
    since_id = get_since_id(search_term)
    output_file_name = get_new_output_file_name(search_term)
    output_file_path = os.path.join(output_folder_path, output_file_name)
    print(f"starting search for {search_term}\n"
    f"saving to {output_file_path}\n"
    f"using since_id: {since_id}")
    searcher = SearchManager(search_terms=[search_term],
                             output_file=output_file_path,
                             bearer_token=BEARER_TOKEN)
    searcher.start_new_search(since_id=since_id)


def get_new_output_file_name(search_term):
    all_results = results_imported + results_not_imported
    pattern = f"{search_term}_(\d+).txt"
    highest_file_number = 0
    for file_name, file_path in all_results:
        match = re.match(pattern, file_name)
        if match:
            file_number = int(match.group(1))
            if file_number > highest_file_number:
                highest_file_number = file_number
    new_file_number = highest_file_number + 1
    new_output_file_name = f"{search_term}_{new_file_number}.txt"
    return new_output_file_name


def get_since_id(search_term):
    previous_file_path = get_previous_file(search_term)
    tweet = get_newest_tweet_from_previous_file(previous_file_path)
    difference = datetime.datetime.now() - tweet.created_at
    print(f"it has been {difference} days since the last scraped tweet was created")
    if difference.days > 7:
        since_id = None
    else:
        since_id = str(tweet.id)
    return since_id
    
    
def get_previous_file(search_term):
    all_results = results_imported + results_not_imported
    pattern = f"{search_term}_(\d+).txt"
    highest_file_number = 0
    file_path_with_highest_number = ""
    for file_name, file_path in all_results:
        match = re.match(pattern, file_name)
        if match:
            file_number = int(match.group(1))
            if file_number > highest_file_number:
                highest_file_number = file_number
                file_path_with_highest_number = file_path
    return file_path_with_highest_number


def get_newest_tweet_from_previous_file(previous_file_path):
    with open(previous_file_path, 'r', encoding='utf-8') as f:
        data = f.read()
    first_line = data.split('\n')[0]
    parsed_response_data = parse_twitter_response(first_line)
    highest_id = 0
    for tweet in parsed_response_data.tweets:
        if tweet.id > highest_id:
            highest_id = tweet.id
            newest_tweet = tweet
    return newest_tweet


if __name__ == "__main__":
    main()

