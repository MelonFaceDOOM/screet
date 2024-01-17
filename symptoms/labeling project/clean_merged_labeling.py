import csv
import re

def main():
    outfile = []
    static_cols = ['id', 'symptom', 'tweet text']
    true_false_cols = ['symptom_mentioned', 'symptom_positively_related_to_vaccine', 'personal_report']
    new_rows = [static_cols+true_false_cols]
    with open('completed labeling/labeling_merged.csv', 'r', encoding="utf-8", newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            new_row = []
            for col in static_cols:
                new_row.append(row[col])
            for col in true_false_cols:
                new_value = clean_true_false(row[col])
                new_row.append(new_value)
            new_rows.append(new_row)

    with open('completed labeling/labeling_cleaned.csv', 'w', encoding="utf-8", newline='') as f:
        writer = csv.writer(f)
        writer.writerows(new_rows)
        

def clean_true_false(raw):
    only_alphabet = re.sub("[^a-zA-Z]+", "", raw)
    if len(only_alphabet) == 0:
        return ''
    elif only_alphabet[0].lower() == 't':
        return 'TRUE'
    elif only_alphabet[0].lower() == 'f':
        return 'FALSE'
    else:
        raise ValueError(f'cannot parse true/false value of {raw}')
        

if __name__ == "__main__":
    main()
