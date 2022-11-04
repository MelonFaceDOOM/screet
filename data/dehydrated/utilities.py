import os


def get_file_paths_from_folder(folder_path):
    file_paths = []
    for subdir, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(folder_path, file)
            file_paths.append(file_path)
    return file_paths


def get_file_names_and_paths_from_folder(folder_path):
    file_names_and_paths = []
    for subdir, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(subdir, file)
            file_names_and_paths.append([file, file_path])
    return file_names_and_paths
