import os
import shutil

def main():
    """takes all relevant csv files out of various subfolders and puts them in one single output folder"""
    rootdir = 'all'
    for subdir, dirs, files in os.walk(rootdir):
        for file in files:
            if file[-3:]=="csv" and file[0]!=".":
                file_path = os.path.join(subdir, file)
                shutil.move(file_path, r"C:\proj\hydrate tweets\ieee\all")

if __name__ == "__main__":
    main()
