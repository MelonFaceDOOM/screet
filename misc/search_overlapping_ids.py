import pandas as pd
from scraping.parse_twitter_response import read_twitter_response, parse_twitter_responses
from utilities import csv_object_from_obj_list


data_source_title = 'panacea'
file_path = 'data/hydrated/panacea_2020_sample_4_hydrated.txt'
    
    
search_results_1 = "data\search_results\imported to db\monkeypox_14.txt"
search_results_2 = "data\search_results\imported to db\monkeypox_15.txt"

twitter_response_data = read_twitter_response(search_results_1)
parsed_response_data = parse_twitter_responses(twitter_responses=twitter_response_data)
cols = parsed_response_data.tweets[0].attrs()
csv_object = csv_object_from_obj_list(parsed_response_data.tweets)
df = pd.read_csv(csv_object, names=cols)
df['created_at'] = pd.to_datetime(df['created_at'])
ids1 = set(df['id'].tolist())

twitter_response_data = read_twitter_response(search_results_2)
parsed_response_data = parse_twitter_responses(twitter_responses=twitter_response_data)
cols = parsed_response_data.tweets[0].attrs()
csv_object2 = csv_object_from_obj_list(parsed_response_data.tweets)
df2 = pd.read_csv(csv_object2, names=cols)
df2['created_at'] = pd.to_datetime(df2['created_at'])
ids2 = set(df2['id'].tolist())

df2 = df2[df2['created_at'] <= df['created_at'].max()]
ids3 = set(df2['id'].tolist())

df2 = df2[df2['id'].isin(df['id'])]
ids4 = set(df2['id'].tolist())

print(len(ids1), len(ids2), len(ids3), len(ids4))