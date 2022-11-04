import os
import shutil


def main():
    """takes all relevant csv files out of various subfolders and puts them in one single output folder"""
    rootdir = r'dailies'
    output_path = r'source_1'
    for subdir, dirs, files in os.walk(rootdir):
        for file in files:
            if file.endswith("clean-dataset.tsv.gz"):
                file_path = os.path.join(subdir, file)
                shutil.move(file_path, output_path)


if __name__ == "__main__":
    main()
