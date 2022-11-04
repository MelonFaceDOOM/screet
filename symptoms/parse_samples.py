import os
import pandas as pd 
from create_labeled_samples import create_labeler_df

pd.options.display.float_format = '{:.0f}'.format

labeled_df = create_labeler_df()
labeled_df = labeled_df.rename({'symptom_mentioned': 'sm1',
                                'symptom_positively_related_to_vaccine': 'sv1',
                                'personal_report': 'pr1'}, axis=1) 
                                  
verified_samples_file = 'labeled_samples.csv'
verified_df = pd.read_csv(verified_samples_file,sep='\t')
verified_df = verified_df[['id', 'symptom_mentioned', 'symptom_positively_related_to_vaccine', 'personal_report']]
verified_df = verified_df.rename({'symptom_mentioned': 'sm2',
                                  'symptom_positively_related_to_vaccine': 'sv2',
                                  'personal_report': 'pr2'}, axis=1) 

df = labeled_df.merge(verified_df, on='id')

labelers = labeled_df['labeler'].unique().tolist()
for l in labelers:
    l_df = labeled_df[labeled_df['labeler']==l]
    print(len(l_df))
    l_df = l_df[~l_df['id'].isin(verified_df['id'])]
    print(len(l_df))
    l_df = l_df[['id', 'text', 'symptom', 'sm1', 'sv1', 'pr1']]
    l_df.to_csv(f'completed labeling\corrections\{l}.csv', index=False)

error_rates = []
for labeler in labelers:
    labeler_df = df[df['labeler']==labeler]
    sm_diff = len(labeler_df[labeler_df['sm1']!=labeler_df['sm2']])
    sv_diff = len(labeler_df[labeler_df['sv1']!=labeler_df['sv2']])
    pr_diff = len(labeler_df[labeler_df['pr1']!=labeler_df['pr2']])
    error_rates.append([labeler, sm_diff+sv_diff+pr_diff, sm_diff, sv_diff, pr_diff])
error_rates.sort(key=lambda x: x[1], reverse=True)
for i in error_rates:
    print(i)
