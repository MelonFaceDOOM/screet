import pandas as pd 
import re
from create_labeled_samples import create_labeler_df


labeled_df = create_labeler_df()
labeled_df = labeled_df.rename({'symptom_mentioned': 'sm1',
                                'symptom_positively_related_to_vaccine': 'sv1',
                                'personal_report': 'pr1'}, axis=1)
# labeled_df = labeled_df.drop_duplicates(subset='text', keep="last")


verified_samples_file = 'labeled_samples.csv'
verified_df = pd.read_csv(verified_samples_file,sep='\t')
verified_df = verified_df[['id', 'text', 'symptom_mentioned', 'symptom_positively_related_to_vaccine', 'personal_report']]
verified_df = verified_df.rename({'symptom_mentioned': 'sm2',
                                  'symptom_positively_related_to_vaccine': 'sv2',
                                  'personal_report': 'pr2'}, axis=1) 
                                  
df_hana = pd.read_csv('hana_partial.csv')
df_hana_2 = pd.read_csv('completed labeling\corrections\hana.csv')
verified_df = verified_df.append(df_hana)
verified_df = verified_df.append(df_hana_2)
verified_df = verified_df.drop_duplicates(subset='text', keep="last")


labelers = labeled_df['labeler'].unique().tolist()


# for l in labelers:
    # l_df = labeled_df[labeled_df['labeler']==l]
    # print(len(l_df))
    # l_df = l_df[~l_df['text'].isin(verified_df['text'])]
    # print(len(l_df))
    # l_df = l_df[['id', 'text', 'symptom', 'sm1', 'sv1', 'pr1']]
    # l_df.to_csv(f'completed labeling\corrections\{l}.csv', index=False)
    
def remove_links(text):
    text = re.sub("(https:\/\/t.co\/[\w]+)", "", text)
    return text
    
df = labeled_df.merge(verified_df[['id', 'sm2', 'sv2', 'pr2']], on='id', how='outer')
df2 = df.copy()
df2['sm'] = df2.apply(lambda x: x['sm2'] if x['sm2'] in [True, False] else x['sm1'], axis=1)
df2['sv'] = df2.apply(lambda x: x['sv2'] if x['sv2'] in [True, False] else x['sv1'], axis=1)
df2['pr'] = df2.apply(lambda x: x['pr2'] if x['pr2'] in [True, False] else x['pr1'], axis=1)
df2 = df2.drop_duplicates(subset='text', keep="last")
df2 = df2[['id', 'text', 'sm', 'sv', 'pr']]
df2['text'] = df2.apply(lambda x: remove_links(x['text']), axis=1)
print(len(df2))
df2 = df2.drop_duplicates(subset='text', keep="last")
print(len(df2))

df2.to_csv('2022-07-11_cleaned.csv', index=False)


# error_rates = []
# for labeler in labelers:
    # labeler_df = df[df['labeler']==labeler]
    # sm_diff = len(labeler_df[labeler_df['sm1']!=labeler_df['sm2']])
    # sv_diff = len(labeler_df[labeler_df['sv1']!=labeler_df['sv2']])
    # pr_diff = len(labeler_df[labeler_df['pr1']!=labeler_df['pr2']])
    # error_rates.append([labeler, sm_diff+sv_diff+pr_diff, sm_diff, sv_diff, pr_diff])
# error_rates.sort(key=lambda x: x[1], reverse=True)
# for i in error_rates:
    # print(i)