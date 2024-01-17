import csv

# excel truncates long ints, so I'm not going to send out the manual coding file to people with the
# original ids. instead there will be new ids starting at 0.
# the output symptom csv file has original id and new id
# this script reads the file, takes those two columns and returns a dict map between the two
# this will be used later to assign true ids to the files sent back by manual coders


def main():
    with open("vaccine_symptom_counts.csv", 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)
    id_map = list_of_lists_to_id_map(rows)
    print(len(id_map))
    

def list_of_lists_to_id_map(list_of_lists):
    rows = [row for row in list_of_lists if row[4]] # only keep rows with tweet id
    id_map = {}
    for row in list_of_lists:
        id_map[row[3]] = row[4]
    return id_map
    

if __name__ == "__main__":
    main()
