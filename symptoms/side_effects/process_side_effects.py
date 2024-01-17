import pandas as pd
from symptoms import QA_symptoms
from config import LOCAL_DB_CREDENTIALS
from db.db_client import establish_psql_connection


excluded_categories = [
    'Investigations',
    'Product issues',
    'Social circumstances',
    'Surgical and medical procedures',
    'Injury, poisoning and procedural complications'
]
with open('symptoms/side_effects/excluded_side_effects.txt', 'r') as f:
    excluded_symptoms = f.read().strip().split('\n')


def extract_side_effects():
    """read reactions source file. this is the file with side_effects that we want to extract"""
    df = pd.read_csv('symptoms/side_effects/source files/reactions.txt', delimiter="$",
                     names=['n1', 'n2', 'u1', 'u2', 'u3', 'name', 'name_fr', 'category', 'category_fr', 'version'])
    df = df[['name', 'category']]
    df = df.drop_duplicates()
    df.to_csv('symptoms/side_effects/side_effects_with_category.txt', index=False)


def get_side_effects_without_exclusions():
    """excludes side effect categories that aren't of interest"""
    df = pd.read_csv('symptoms/side_effects/side_effects_with_category.txt')
    df = df.sort_values(by=['category', 'name'])
    df = df.dropna()
    df = df[~df['category'].isin(excluded_categories)]
    df = df[~df['name'].str.contains('|'.join(excluded_symptoms), case=False)]
    return df


def get_side_effects_from_excluded_categories():
    """excludes side effect categories that aren't of interest"""
    df = pd.read_csv('symptoms/side_effects_with_category.txt')
    df = df.sort_values(by=['category', 'name'])
    return df[df['category'].isin(excluded_categories)]


def save_tweets_for_side_effects():
    """saves a csv file with tweets that contain side effects and were also not found by the existing symptom query
    list"""
    df = get_side_effects_without_exclusions()
    symptoms = list(set(df['name'].tolist()))
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    sqa = QA_symptoms.SymptomQA(conn=conn)
    new_querylist = sqa.symptoms_to_queries(symptoms)
    df_old_not_in_new, df_new_not_in_old = sqa.compare_querylist(new_querylist)
    df_new_not_in_old.to_csv('symptoms/side_effects/new_tweets_from_side_effects.csv', index=False)


def analyse_tweet_search_results():
    df = pd.read_csv('symptoms/side_effects/new_tweets_from_side_effects.csv')
    df = df['query'].value_counts()
    print(df.head(n=100))
