import os


def main():
    """source files have id and sentiment score.
    This script removes sentiment and saves only the ids."""
    source_files_folder = "source/source_6"
    output_folder = "ids/ids_2022_2"
    file_names_and_paths = get_file_names_and_paths_from_folder(source_files_folder)
    for file_name, file_path in file_names_and_paths:
        ids = get_ids_from_file(file_path)
        ids_text = '\n'.join(ids)
        output_file_name = file_name.split('.')[0] + ".txt"
        output_file_path = os.path.join(output_folder, output_file_name)
        with open(output_file_path, 'w') as f:
            f.write(ids_text)
        

def get_ids_from_file(file_path):
    with open(file_path, 'r') as f:
        lines = f.readlines()
    tweet_ids = []
    for line in lines[1:]:
        line = line.split('\t')
        tweet_ids.append(line[0])
    return tweet_ids
    
    
def get_file_names_and_paths_from_folder(folder_path):
    file_names_and_paths = []
    for subdir, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(subdir, file)
            file_names_and_paths.append([file, file_path])
    return file_names_and_paths

    
    
if __name__ == "__main__":
    main()
    
