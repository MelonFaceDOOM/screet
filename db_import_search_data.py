import os
import shutil

from config import LOCAL_DB_CREDENTIALS
from psycopg2.extras import RealDictCursor
from utilities import get_only_file_paths_from_folder
from scraping.parse_twitter_response import read_twitter_response, parse_twitter_responses
from db.db_client import PsqlClient, establish_psql_connection


folder = '/data/screet_data/search_results'
imported_folder = '/data/screet_data/search_results/imported to db'
data_sources = ["polio", "monkeypox"]


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
    for file_path in files:
        file_name = os.path.basename(file_path)
        dst_file_path = os.path.join(imported_folder, file_name)
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
            shutil.move(file_path, dst_file_path)


if __name__ == "__main__":
    main()
