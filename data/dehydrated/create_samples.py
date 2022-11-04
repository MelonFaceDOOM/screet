from read_data_partitions import PartitionedData


def main():
    pass


class PartitionedDataSampler:
    def __init__(self, partitioned_data: PartitionedData):
        self.partitioned_data = partitioned_data
        full_samples = self.calc_full_samples()
        self.number_of_full_samples = self.calc_full_samples()
        self.calc_lines_for_each_file_in_final_sample()
        
    def calc_full_samples(self):
        number_of_samples = int(self.partitioned_data.total_line_count / self.partitioned_data.total_sample_line_count)
        return number_of_samples
        
    def calc_lines_for_each_file_in_final_sample(self):
        for data_partition in self.partitioned_data:
            lines_in_full_samples = data_partition.sample_line_count * self.number_of_full_samples
            lines_remaining = data_partition.line_count - lines_in_full_samples
            data_partition.lines_in_final_sample = lines_remaining
            
    def build_samples(self):
        for sample in self.build_full_samples():
            yield sample
        final_sample = self.build_final_sample()
        yield final_sample
        
    def build_full_samples(self):
        for sample_number in range(self.number_of_full_samples):
            yield self.build_full_sample(sample_number=sample_number)
        
    def build_full_sample(self, sample_number):
        sample = []
        for partition in self.partitioned_data:
            partition_sample = self.get_sample_from_partition(partition=partition, start_pos=sample_number)
            sample += partition_sample
        return sample
        
    def get_sample_from_partition(self, partition, start_pos):
        lines = self.get_lines_from_partition(partition)
        step_size = int(len(lines)/partition.sample_line_count)
        sample = lines[start_pos::step_size]
        return sample
        
    def build_final_sample(self):
        sample = []
        for partition in self.partitioned_data:
            sample += self.get_final_sample_from_partition(partition)
        return sample
        
    def get_final_sample_from_partition(self, partition):
        lines = self.get_lines_from_partition(partition)
        final_sample = lines[-partition.lines_in_final_sample:]
        return final_sample
        
    def get_lines_from_partition(self, partition):
        with open(partition.file_path, 'r') as f:
            lines = f.read().splitlines() 
        return lines

    


if __name__ == "__main__":
    main()
