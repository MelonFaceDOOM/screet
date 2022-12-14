import os

"""samples were given an extra \n at the end.
When reading to list, this results in an empty string as the final item in the list.
This script removes the trailing \n in sample files
"""


def main():
    samples_folder = "samples"
    file_paths = get_file_paths_from_folder(samples_folder)
    for file_path in file_paths:
        fix_sample_file(file_path)


def fix_sample_file(sample_file):
    with open(sample_file, 'r', encoding='utf-8') as f:
        sample_text = f.read()
    if sample_text[-1:] == "\n":
        sample_text = sample_text[:-1]
        with open(sample_file, 'w', encoding='utf-8') as f:
            f.write(sample_text)


def get_file_paths_from_folder(folder_path):
    file_paths = []
    for subdir, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(folder_path, file)
            file_paths.append(file_path)
    return file_paths


if __name__ == "__main__":
    main()
