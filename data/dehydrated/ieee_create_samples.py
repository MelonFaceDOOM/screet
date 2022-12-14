import os
import re
from read_data_partitions import read_data_partitions
from create_samples import PartitionedDataSampler
from utilities import get_file_paths_from_folder


INPUT_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ieee/ids")
OUTPUT_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ieee/samples")
if not os.path.isdir(OUTPUT_FOLDER):
    raise ValueError(f'output folder {OUTPUT_FOLDER} does not exist.')
N_SAMPLES = 10
SAMPLE_SIZE = 2_000_000


def main():
    partition_file_paths = get_file_paths_from_folder(INPUT_FOLDER)
    partition_file_paths = keep_files_in_date_range(partition_file_paths)
    partioned_data = read_data_partitions(partition_file_paths, SAMPLE_SIZE)
    sampler = PartitionedDataSampler(partitioned_data=partioned_data, n_samples=N_SAMPLES)
    counter = 0
    for sample in sampler.build_samples():
        sample = '\n'.join(sample)
        save_sample(sample, sample_number=counter)
        counter += 1


def keep_files_in_date_range(partition_file_paths):
    files_to_keep = []
    for file_path in partition_file_paths:
        if file_is_in_date_range(file_path):
            files_to_keep.append(file_path)
    return files_to_keep


def file_is_in_date_range(file_name):
    set_number = get_data_set_number(file_name)
    if set_number and set_number >= 289:
        return True
    return False
    
    
def get_data_set_number(file_name):
    match = re.search('corona_tweets_(\d+)', file_name)
    if match:
        return int(match.group(1))
    else:
        return None

    
def save_sample(sample, sample_number):
    file_path = os.path.join(OUTPUT_FOLDER, f"sample_{sample_number}.txt")
    with open(file_path, 'w') as f:
        for element in sample:
            f.write(element + "\n")

            
if __name__ == "__main__":
    main()
