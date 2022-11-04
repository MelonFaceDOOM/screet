import os
import pandas as pd 


def main():
    pd.options.display.float_format = '{:.0f}'.format
    samples_df = create_labeled_samples_df()
    samples_df.to_csv('labeled_samples.csv')
    
    
def create_labeled_samples_df():
    labeler = create_labeler_df()

    # get labeler list
    labelers = cleaned_df['labeler'].unique().tolist()

    # build df with samples from each labeler, starting with first labeler
    samples_df = cleaned_df[cleaned_df['labeler']==labelers[0]]
    samples_df = samples_df.sample(n=100)

    # add sample rows from each subsequent labeler
    for labeler in labelers[1:]:
        df = cleaned_df[cleaned_df['labeler']==labeler]
        df = df.sample(n=100)
        samples_df = samples_df.append(df)
    
    return samples_df
    
def create_labeler_df():
    # build file list
    dirname = "completed labeling"
    files = []
    for (dirpath, dirnames, filenames) in os.walk(dirname):
        files.extend(filenames)
        break

    # create df from first file
    first_file = os.path.join(dirname, files[0])
    labeler_df = pd.read_csv(first_file)
    labeler_df['labeler'] = first_file.split('_')[1].split('.')[0]

    # merge each other files into df
    for f in files[1:]:
        f = os.path.join(dirname, f)
        df = pd.read_csv(f)
        df['labeler'] = f.split('_')[1].split('.')[0]
        labeler_df = labeler_df.append(df, ignore_index=True)
        
    # take cleaned t/f values from other file
    cleaned_file = "completed labeling\merged\labeling_cleaned.csv"
    cleaned_df = pd.read_csv(cleaned_file)
    cleaned_df = cleaned_df.merge(labeler_df[['id','labeler']], on='id')
        
    # get old id from id_map_file. also get text since some new text is truncated
    id_map_file = "vaccine_symptom_counts_2.csv"
    id_map = pd.read_csv(id_map_file, header=None, names=['cat','symptom','count','id','old_id','text'])
    cleaned_df = cleaned_df.merge(id_map[['id','old_id','text']], on='id')

    # keep necessary columns
    cleaned_df = cleaned_df[['id', 'old_id', 'labeler', 'text', 'symptom', 'symptom_mentioned', 'symptom_positively_related_to_vaccine', 'personal_report']]
    
    # drop rows with nulls on t/f vars
    cleaned_df = cleaned_df[cleaned_df['symptom_mentioned'].notnull()
                & cleaned_df['symptom_positively_related_to_vaccine'].notnull()
                & cleaned_df['personal_report'].notnull()]
    return cleaned_df

if __name__ == "__main__":
    main()
