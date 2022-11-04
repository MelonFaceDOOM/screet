import os
import datetime
from dehydrated.read_data_partitions import read_data_partitions
from dehydrated.create_samples import PartitionedDataSampler
from dehydrated.utilities import get_file_paths_from_folder


INPUT_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "panacea/ids")
OUTPUT_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "panacea/samples")
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
        file_name = file_path.split('\\')[-1]
        if file_is_in_date_range(file_name):
            files_to_keep.append(file_path)
    return files_to_keep


def file_is_in_date_range(file_name):
    date = file_name[:10]
    date = datetime.date.fromisoformat(date)
    min_date = datetime.date.fromisoformat("2021-01-01")
    if date >= min_date:
        return True
    return False

    
def save_sample(sample, sample_number):
    file_path = os.path.join(OUTPUT_FOLDER, f"panacea_sample_{sample_number}.txt")
    with open(file_path, 'w+') as f:
        for element in sample:
            f.write(element + "\n")


if __name__ == "__main__":
    main()
