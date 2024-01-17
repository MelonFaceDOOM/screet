import re
import time
import shutil
import os
from config import LOCAL_DB_CREDENTIALS
from utilities import get_file_names_and_paths_from_folder
from scraping.parse_twitter_response import read_twitter_response, parse_twitter_responses
from db.db_client import PsqlClient, establish_psql_connection


hydrated_folder = "D:/work/temp/screet_data/hydrated"
imported_folder = "D:/work/temp/screet_data/hydrated_imported"
data_source_title = 'panacea'


def main():
    import_all_panacea_data()


def import_all_panacea_data():
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    client = PsqlClient(conn=conn)
    file_paths = get_hydrated_panacea_sample_file_paths()
    for file_path in file_paths:
        file_name = os.path.basename(file_path)
        dst_file_path = os.path.join(imported_folder, file_name)
        twitter_response_data = read_twitter_response(file_path)
        parsed_response_data = parse_twitter_responses(twitter_responses=twitter_response_data)
        for tweet in parsed_response_data.tweets:
            tweet.source = data_source_title
        t1 = time.perf_counter()
        client.save_parsed_response_data(parsed_response_data)
        t2 = time.perf_counter()
        print(f'saved {file_path} to db', t2-t1)
        shutil.move(file_path, dst_file_path)


def get_hydrated_panacea_sample_file_paths():
    """returns all completely hydrated panacea sample files from the hydrated folder."""
    paths = []
    files_and_paths = get_file_names_and_paths_from_folder(hydrated_folder)
    progress_file_pattern = "(.+)_progress.txt"
    hydrated_file_pattern = "(.+)_hydrated.txt"
    progress_file_prefix = ""
    for file, path in files_and_paths:
        match = re.match(pattern=progress_file_pattern, string=file)
        if match:
            progress_file_prefix = match.group(1)
    for file, path in files_and_paths:
        match = re.match(pattern=hydrated_file_pattern, string=file)
        if match:
            file_prefix = match.group(1)
            if file_prefix != progress_file_prefix:
                paths.append(path)
    return paths


if __name__ == "__main__":
    main()
