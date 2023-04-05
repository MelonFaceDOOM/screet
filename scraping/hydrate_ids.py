import re
import os
from config import BEARER_TOKEN
from utilities import get_file_names_and_paths_from_folder
from scraping.twitter_api_requestor import MultiFileHydrator


def main():
    hydrate_id_files()


def hydrate_id_files():
    dehydrated_source_folder = "data/dehydrated/panacea/samples/samples_2020"
    output_folder = "/data/screet_data/hydrated"
    source_files_and_paths = get_file_names_and_paths_from_folder(dehydrated_source_folder)
    already_hydrated_file_names = get_already_hydrated_file_names(output_folder)
    files_to_hydrate = get_files_to_hydrate(source_files_and_paths, already_hydrated_file_names)
    files_to_hydrate = sorted(files_to_hydrate)
#    for f in files_to_hydrate:
#        print(f)
    hydrator = MultiFileHydrator(files_to_hydrate=files_to_hydrate,
                                 output_folder=output_folder,
                                 bearer_token=BEARER_TOKEN)
    hydrator.hydrate_files_and_save_output()
            
            
def get_already_hydrated_file_names(output_folder):
    pattern = "(.+)_hydrated.txt"
    hydrated_and_imported_files = get_file_names_and_paths_from_folder("/data/screet_data/hydrated_imported")
    hydrated_not_imported_files = get_file_names_and_paths_from_folder("/data/screet_data/hydrated")
    hydrated_file_paths = hydrated_and_imported_files + hydrated_not_imported_files
    already_hydrated = []
    for file_name, file_path in hydrated_file_paths:
        match = re.match(pattern, file_name)
        if match:
            source_file_name = match.group(1) + ".txt"
            progress_file_name = source_file_name.split('.')[0] + "_progress.txt"
            progress_file_path = os.path.join(output_folder, progress_file_name)
            if os.path.isfile(progress_file_path):
                pass  # do not classify as "already_hydrated"
            else:
                already_hydrated.append(source_file_name)
    return already_hydrated


def get_files_to_hydrate(source_files_and_paths, already_hydrated_file_names):
    files_to_hydrate = []
    for file_name, file_path in source_files_and_paths:
        if file_name not in already_hydrated_file_names:
            files_to_hydrate.append(file_path)
    return files_to_hydrate
    


if __name__ == "__main__":
    main()
