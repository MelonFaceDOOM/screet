import os
import re
import shutil
from config import LOCAL_DB_CREDENTIALS
from utilities import get_file_paths_from_folder_and_subfolders, get_subfolder_paths_from_folder
from scraping.parse_twitter_response import read_twitter_response, parse_twitter_responses
from db.db_client import PsqlClient, establish_psql_connection
from scraping.search import locate_progress_file


not_imported = 'D:/screet_data/search_results/not imported'
imported_folder = 'D:/screet_data/search_results/imported to db'


def main():
    save_files_in_search_results_folder()
    
    
def get_data_file_paths_from_folder(folder):
    file_paths = get_file_paths_from_folder_and_subfolders(folder)
    data_file_paths = []
    for file_path in file_paths:
        if not "_progress" in file_path:
            progress_file = locate_progress_file(file_path)
            if not progress_file:
                data_file_paths.append(file_path)
    return data_file_paths
    
    
def save_files_in_search_results_folder():
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    client = PsqlClient(conn=conn)
    unimported_search_folders = get_subfolder_paths_from_folder(not_imported)
    for folder in unimported_search_folders:
        data_source = re.split(r'[/\\]', folder)[-1]  # the data_source is the name of the folder.
        # i.e. the "monkeypox" folder will hold 'mpox' and 'monkeypox' search results.
        dst_folder_path = os.path.join(imported_folder, data_source)
        if not os.path.exists(dst_folder_path):
            os.makedirs(dst_folder_path)
        file_paths = get_data_file_paths_from_folder(folder)
        for file_path in file_paths:
            file_name = os.path.basename(file_path)
            dst_file_path = os.path.join(dst_folder_path, file_name)
            twitter_response_data = read_twitter_response(file_path)
            parsed_response_data = parse_twitter_responses(twitter_responses=twitter_response_data)
            for tweet in parsed_response_data.tweets:
                tweet.source = data_source
            client.save_parsed_response_data(parsed_response_data)
            print(f'saved {file_path} to db')
            shutil.move(file_path, dst_file_path)


if __name__ == "__main__":
    main()
