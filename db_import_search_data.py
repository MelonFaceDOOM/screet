import csv
import sys
import os

from config import LOCAL_DB_CREDENTIALS
from utilities import get_file_names_and_paths_from_folder, get_only_file_paths_from_folder
from scraping.parse_twitter_response import read_twitter_response, parse_twitter_responses
from db.db_client import PsqlClient, establish_psql_connection
from db.models import ParsedResponseData


folder = 'C:\proj\covid_scraping\screet\data\search_results'


def main():
    save_files_in_search_results_folder()
    
    
def get_data_files_from_folder(folder):
    files = get_only_file_paths_from_folder(folder)
    data_files = []
    for file in files:
        if "progress" not in file:
            data_files.append(file)
    return data_files
    
    
def save_files_in_search_results_folder():
    files = get_data_files_from_folder(folder)
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    client = PsqlClient(conn=conn)
    data_sources = ["polio", "monkeypox"]
    for file_path in files:
        for data_source in data_sources:
            if data_source in file_path:
                break
        else:
            data_source = None
        if data_source:
            twitter_response_data = read_twitter_response(file_path)
            parsed_response_data = parse_twitter_responses(twitter_responses=twitter_response_data)
            for tweet in parsed_response_data.tweets:
                tweet.source = data_source
            client.save_parsed_response_data(parsed_response_data)
            print(f'saved {file_path} to db')


if __name__ == "__main__":
    main()
