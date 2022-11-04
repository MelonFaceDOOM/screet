import os
import sys

def main():
    input_folder = "ids_2020"
    output_folder = "ids_2020_2"
    file_names_and_paths = get_file_names_and_paths_from_folder(input_folder)
    for file_name, file_path in file_names_and_paths:
        fixed_file_contents = fix_file(file_path)
        output_path = os.path.join(output_folder, file_name)
        with open(output_path, 'w') as f:
            f.write(fixed_file_contents)
        


def fix_file(sample_file):
    with open(sample_file, 'r', encoding='utf-8') as f:
        lines = f.read().splitlines()
    lines = list(filter(lambda val: val !=  '', lines))
    lines = '\n'.join(lines)
    return lines
    


def get_file_names_and_paths_from_folder(folder_path):
    file_names_and_paths = []
    for subdir, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(subdir, file)
            file_names_and_paths.append([file, file_path])
    return file_names_and_paths



if __name__ == "__main__":
    main()
