import csv
import re


original_misinfo_file = r'other_data/COVIDGlobal Misinformation Dashboard 2020-22_Page 1_Table.csv'
misinfo_file = r'other_data/misinfo_with_domain.csv'
misinfo_backup_file = r'other_data/misinfo_with_domain_bkp.csv'
domain_counts_file = 'domains_with_counts.csv'

def main():
    pass


def get_domain_counts_and_save_to_csv():
    domains = []
    with open(misinfo_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        for line in reader:
            domains.append(line[4])
    unique_domains = list(set(domains))
    domain_counts = []
    for domain in unique_domains:
        domain_count = domains.count(domain)
        domain_counts.append([domain, domain_count])
    domain_counts.sort(key=lambda x: x[1], reverse=True)
    domain_counts = [['domain', 'count']] + domain_counts
    with open(domain_counts_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(domain_counts)
        
        
def create_new_file_with_domain():
    lines_with_domain = add_domain_to_source_file()
    with open(misinfo_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(new_lines)
        
        
        
def add_domain_to_source_file():
    new_lines = [["Review Date", "Claim (auto-translated into English)", "Rating Provided by Fact-checker", "Review Article", "Domain"]]
    with open(original_misinfo_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        for line in reader:
            url = line[3]
            domain = url_to_domain(url)
            line.append(domain)
            new_lines.append(line)
    return new_lines
    
        
def url_to_domain(url):
    pattern = '(?:https?:\/\/)?((?:[\w-]+\.)+(?:\w+)+).*'
    match = re.match(pattern, url)
    if not match:
        return None
    return match.group(1)    
    
        
if __name__ == "__main__":
    main()
