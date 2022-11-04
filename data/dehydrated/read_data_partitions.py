from dataclasses import dataclass
from utilities import get_file_paths_from_folder


def main():
    """this is just a test. other funcs are called in other modules."""
    source_data_folder = r"test_set"
    total_sample_line_count = 2_000_000
    partition_file_paths = get_file_paths_from_folder(source_data_folder)
    data_partitions = read_data_partitions(partition_file_paths, total_sample_line_count)
    total_sample_lines = sum([dp.sample_line_count for dp in data_partitions])
    print(total_sample_lines)
    
    
def read_data_partitions(partition_file_paths, total_sample_line_count):
    """look at sample files in partition_file_paths and determines how many lines need to be pulled from each file
    in order to create a sample with lines == total_sample_line_count"""
    partitioned_data = PartitionedData(partition_file_paths, total_sample_line_count)
    return partitioned_data

    
@dataclass
class DataPartition:
    """stores metadata about a newline-separated data file that is a member of a larger dataset.
    sample_line_count is present to support the ability to extract a sample of the partition"""
    file_path: str
    line_count: int
    proportion: float = 0
    sample_line_count: int = 0
    

class PartitionedData:
    """stores metadata about a set of newline-separated data files.
    Calculates how many lines from each file will be used to produce a sample of the total set."""
    def __init__(self, partition_file_paths: list, total_sample_line_count=0):
        self.total_line_count = 0
        self.total_sample_line_count = total_sample_line_count
        self.partitions = []
        self.read_source_files(partition_file_paths)
        self.calc_each_partition_proportion()
        if total_sample_line_count > 0:
            self.calc_sample_lines_for_each_partition()
        
    def read_source_files(self, partition_file_paths: list):
        for file_path in partition_file_paths:
            line_count = get_file_line_count(file_path)
            partition = DataPartition(file_path=file_path, line_count=line_count)
            self.add_partition(partition)
        
    def add_partition(self, partition: DataPartition):
        self.partitions.append(partition)
        self.calc_total_lines()
    
    def calc_total_lines(self):
        self.total_line_count = sum([p.line_count for p in self.partitions])
        
    def calc_each_partition_proportion(self):
        for partition in self.partitions:
            partition.proportion = partition.line_count/self.total_line_count
            
    def calc_sample_lines_for_each_partition(self):
        for partition in self.partitions:
            partition.sample_line_count = int(partition.proportion * self.total_sample_line_count) # int() always rounds down
        self.force_sample_lines_to_target()
        
    def force_sample_lines_to_target(self):
        current_total_sample_lines = self.calc_total_sample_lines_in_partitions()
        sample_deficit = self.total_sample_line_count - current_total_sample_lines
        i = 0
        while sample_deficit:
            self.partitions[i].sample_line_count += 1
            sample_deficit -= 1
            if i >= len(self.partitions):
                i = 0
            else:
                i += 1
        
    def calc_total_sample_lines_in_partitions(self):
        return sum([p.sample_line_count for p in self.partitions])
    
    def __iter__(self):
        return iter(self.partitions)
        
    
def get_file_line_count(file_path):
    """returns count of '\n' found in the file as an int"""
    with open(file_path, 'r') as f:
        text = f.read()
        line_count = text.count('\n')
    return line_count
    

if __name__ == "__main__":
    main()
