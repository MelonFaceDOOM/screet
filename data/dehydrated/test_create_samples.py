import os
import datetime
from read_data_partitions import read_data_partitions
from create_samples import PartitionedDataSampler
from utilities import get_file_paths_from_folder


INPUT_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "panacea\ids_2020_2")
OUTPUT_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "panacea\samples_2020")
if not os.path.isdir(OUTPUT_FOLDER):
    raise ValueError(f'output folder {OUTPUT_FOLDER} does not exist.')
SAMPLE_SIZE = 2_000_000


def main():

    partition_file_paths = get_file_paths_from_folder(INPUT_FOLDER)
    partitioned_data = read_data_partitions(partition_file_paths, SAMPLE_SIZE)
    sampler = PartitionedDataSampler(partitioned_data=partitioned_data)
    
    # for data_partition in sampler.partitioned_data.partitions:
        # lines_in_samples = 0
        # lines_in_samples += sampler.number_of_full_samples * data_partition.sample_line_count
        # lines_in_samples += data_partition.lines_in_final_sample
        # print(lines_in_samples)
        # print(data_partition.line_count)
        # print()
        
        
    counter = 0
    for sample in sampler.build_samples():
        sample_string = '\n'.join(sample)
        save_sample(sample_string, sample_number=counter)
        counter += 1

def save_sample(sample_string, sample_number):
    file_path = os.path.join(OUTPUT_FOLDER, f"panacea_2020_sample_{sample_number}.txt")
    with open(file_path, 'w+') as f:
        f.write(sample_string)


if __name__ == "__main__":
    main()
