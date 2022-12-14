import os
from twitter.utilities import get_file_names_and_paths_from_folder


def main():
    """source files have id and sentiment score.
    This script removes sentiment and saves only the ids."""
    source_files_folder = r"all"
    output_folder = r"ids"
    file_names_and_paths = get_file_names_and_paths_from_folder(source_files_folder)
    for file_name, file_path in file_names_and_paths:
        ids = get_ids_from_file(file_path)
        ids_text = '\n'.join(ids)
        output_file_path = os.path.join(output_folder, file_name)
        with open(output_file_path, 'w') as f:
            f.write(ids_text)

    
def get_ids_from_file(file_path):
    with open(file_path, 'r') as f:
        lines = f.readlines()
    tweet_ids = []
    for line in lines:
        line = line.split(',')
        tweet_ids.append(line[0])
    return tweet_ids


if __name__ == "__main__":
    main()
    