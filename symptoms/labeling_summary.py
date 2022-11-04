import csv
import re
import pandas as pd
import sys

df = pd.read_csv('completed labeling/labeling_merged.csv')
mentioned = df[['symptom', 'symptom_mentioned', 'id']].groupby(['symptom', 'symptom_mentioned']).count()
related = df[['symptom', 'symptom_positively_related_to_vaccine', 'id']].groupby(['symptom', 'symptom_positively_related_to_vaccine']).count()
personal = df[['symptom', 'personal_report', 'id']].groupby(['symptom', 'personal_report']).count()



symptom_list = df['symptom'].unique().tolist()
print(len(symptom_list))
symptom_summaries = [['symptom', 'symptom_count', 'mentioned', 'related', 'personal']]
for symptom in symptom_list:
    symptom_df = df[df['symptom']==symptom]
    
    symptom_count = len(symptom_df)
    
    mentioned_true_count = len(symptom_df[symptom_df['symptom_mentioned']==True])
    mentioned_true_ratio = mentioned_true_count / symptom_count

    related_true_count = len(symptom_df[symptom_df['symptom_positively_related_to_vaccine']==True])
    related_true_ratio = related_true_count / symptom_count
    
    personal_true_count = len(symptom_df[symptom_df['personal_report']==True])
    personal_true_ratio = related_true_count / symptom_count
    
    symptom_stats = [symptom, symptom_count, mentioned_true_ratio, related_true_ratio, personal_true_ratio]
    # print(symptom_stats)
    symptom_summaries.append(symptom_stats)
new_df = pd.DataFrame(symptom_summaries[1:], columns=symptom_summaries[0])
# new_df = pd.DataFrame(symptom_summaries)
# print(new_df.head())
# new_df = new_df.rename(columns=new_df.iloc[0]).drop(new_df.index[0])

new_df = new_df.sort_values(by=['mentioned'], ascending=False)
print(new_df.head(10))
new_df = new_df.sort_values(by=['related'], ascending=False)
print(new_df.head(10))
new_df = new_df.sort_values(by=['personal'], ascending=False)
print(new_df.head(10))
    # symptom_count = df[df['symptom']==symptom].count()

    # mentioned_true_count = df[(df['symptom']==symptom) & (df['symptom_mentioned']==True)].count()
    # mentioned_true_ratio = mentioned_true_count / symptom_count
    
    # related_true_count = df[(df['symptom']==symptom) & (df['symptom_positively_related_to_vaccine']==True)].count()
    # related_true_ratio = related_true_count / related_total_count
    
    # mentioned_true_count = mentioned[(mentioned['symptom']==symptom) & (mentioned['symptom_mentioned']==True)].count()
    # mentioned_total_count = mentioned[mentioned['symptom']==symptom].count()
    # mentioned_true_ratio = mentioned_true_count / mentioned_total_count

# def main():
    # outfile = []
    # static_cols = ['id', 'symptom', 'tweet text']
    # true_false_cols = ['symptom_mentioned', 'symptom_positively_related_to_vaccine', 'personal_report']
    # new_rows = [static_cols+true_false_cols]
    # with open('completed labeling/labeling_merged.csv', 'r', encoding="utf-8", newline='') as f:
        # reader = csv.DictReader(f)
        # for row in reader:
            # new_row = []
            # for col in static_cols:
                # new_row.append(row[col])
            # for col in true_false_cols:
                # new_value = clean_true_false(row[col])
                # new_row.append(new_value)
            # new_rows.append(new_row)

    # with open('completed labeling/labeling_cleaned.csv', 'w', encoding="utf-8", newline='') as f:
        # writer = csv.writer(f)
        # writer.writerows(new_rows)
        

# def clean_true_false(raw):
    # only_alphabet = re.sub("[^a-zA-Z]+", "", raw)
    # if len(only_alphabet) == 0:
        # return ''
    # elif only_alphabet[0].lower() == 't':
        # return 'TRUE'
    # elif only_alphabet[0].lower() == 'f':
        # return 'FALSE'
    # else:
        # raise ValueError(f'cannot parse true/false value of {raw}')
        

# if __name__ == "__main__":
    # main()
