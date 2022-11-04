import csv
import json
import pandas as pd
import plotly.express as px

from config import LOCAL_DB_CREDENTIALS
from db.db_client import PsqlClient, establish_psql_connection

symptom_file = "other_data\COVID-Twitter-Symptom-Lexicon.tsv"
aefis_file = r"symptoms\aefis.txt"


def main():
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    # client = PsqlClient(conn)
    mld = get_manually_labeled_data()
    mld.to_csv('manually_labeled_data_ratios.csv')
    # mld = mld.rename(index={'dizziness/disorientation/confusion': 'dizziness'})
    # fig = px.bar(mld, x=mld['symptom_positively_related_to_vaccine'].index,
                 # y=mld['symptom_positively_related_to_vaccine'],
                 # labels={
                     # "symptom_positively_related_to_vaccine": "Tweet Count",
                     # "category": "Symptom",
                 # },)
    # fig.update_layout(font=dict(
        # size=18
    # ))
    # fig.show()
    # print(mld['symptom_positively_related_to_vaccine'][:10])


def save_negatives(conn):
    with open("symptoms/vaccine_symptom_ids.txt", 'r') as f:
        tweet_ids = f.read().splitlines()
    tweet_ids = tuple(set([int(tweet_id) for tweet_id in tweet_ids]))

    cur = conn.cursor()
    cur.execute('''SELECT id, tweet_text FROM vaccine_tweets WHERE id NOT IN %s LIMIT 60000''',(tweet_ids,))
    results = cur.fetchall()
    with open("symptoms/negatives.csv", 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(results)


def get_manually_labeled_data():
    initial_search_results = import_initial_search_results()
    top_symptoms_before = initial_search_results.groupby('symptom').count().sort_values(by=['id'], ascending=False)
    manually_labeled_data = import_manually_labeled_data()
    true_ratios = merge_and_add_true_positive_ratios(initial_search_results, manually_labeled_data)
    return true_ratios


def import_initial_search_results():
    df = pd.read_csv('symptoms/vaccine_symptom_counts_2.csv', names=['category', 'symptom', 'n', 'id',
                                                                      'tweet_id', 'tweet_text'])
    df = df[df['id'].notnull()]
    # df1 = df1.drop(['symptom', 'tweet_id'], axis=1)
    df = df[['symptom', 'id']]
    return df


def import_manually_labeled_data():
    df = pd.read_csv('symptoms/completed labeling/merged/labeling_cleaned.csv',
                      usecols=['id', 'symptom_mentioned',
                              'symptom_positively_related_to_vaccine',
                              'personal_report'])

    df = df[df['symptom_mentioned'].notnull()
            & df['symptom_positively_related_to_vaccine'].notnull()
            & df['personal_report'].notnull()]
    return df


def merge_and_add_true_positive_ratios(initial_search_results, manually_labeled_data):
    merged = pd.merge(initial_search_results, manually_labeled_data, on='id', how='inner')
    merged = merged.drop('id', axis=1)
    true_ratios = merged.groupby('symptom').sum()
    true_ratios['mentioned_ratio'] = true_ratios.apply(lambda x: get_ratio_for_category(merged, x.name,
                                                      x['symptom_mentioned']), axis=1)
    true_ratios['vaccine_ratio'] = true_ratios.apply(lambda x: get_ratio_for_category(merged, x.name,
                                                      x['symptom_positively_related_to_vaccine']), axis=1)
    true_ratios['personal_ratio'] = true_ratios.apply(lambda x: get_ratio_for_category(merged, x.name,
                                                      x['personal_report']), axis=1)
    true_ratios = true_ratios.sort_values(by=['symptom_positively_related_to_vaccine'], ascending=False)
    return true_ratios


def get_ratio_for_category(merged_df, symptom, true_count):
    total_count = (merged_df['symptom'] == symptom).sum()
    return true_count / total_count


def produce_vaccine_tweet_counts(client):
    # client.create_vaccine_tweets_table()
    symptoms = get_symptoms()
    output = []
    id_counter = 0  # excel rounds big ints, so i'm adding a new id starting at 0
    for symptom in symptoms:
        for symptom_synonym in symptoms[symptom]:
            formatted_query = format_query(symptom_synonym)
            results = client.search_vaccine_tweet_text(formatted_query)
            symptom_output = []
            if not results:
                symptom_output.append([symptom, symptom_synonym, len(results), '', '', ''])
            for result in results[:100]:
                symptom_output.append(
                    [symptom, symptom_synonym, len(results), id_counter, result['id'], result['tweet_text']])
                id_counter += 1
            output += symptom_output

    with open("vaccine_symptom_counts_2.csv", 'w', encoding='utf-8', newline='\n') as f:
        writer = csv.writer(f)
        writer.writerows(output)


def get_tweet_ids_with_symptoms(client):
    client.create_vaccine_tweets_table()
    symptoms = get_symptoms()
    ids = []
    for symptom in symptoms:
        for symptom_synonym in symptoms[symptom]:
            formatted_query = format_query(symptom_synonym)
            results = client.search_vaccine_tweet_text(formatted_query)
            symptom_ids = [str(r['id']) for r in results]
            ids += symptom_ids
    return ids


def format_query(raw):
    """this deals with the issue of the search not accepting apostrophes.
       i.e. this func will transform can't -> can t
       can will match positively for both can and can't in source tweet text.
       better solution would be to remove apostrophes from the source, and add cant as
       a word to the dict, then replace this func with one that removes apostrophes """
    words = raw.split(" ")
    words = [word.split("'")[0] for word in words]
    formatted_query = " ".join(words)
    return formatted_query


def get_symptoms():
    with open(aefis_file, 'r', encoding='utf-8') as f:
        symptoms = json.loads(f.read())
    return symptoms


def tsv_to_symptom_dict():
    symptoms = {}
    with open(symptom_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader)
        for row in reader:
            if row[0].lower() in symptoms:
                symptoms[row[0].lower()].append(row[2].lower())
            else:
                symptoms[row[0].lower()] = [row[2].lower()]
    return symptoms


# def symptom_map():
# symptom_id_map = {}
# symptoms = read_symptoms()
# for symptom in symptoms:
#     symptom = re.sub(r'[^a-z]', '', symptom)
#     if symptom:
#         results = client.search_tweet_text(symptom)
#         symptom_id_map[symptom] = results
#
# symptom_id_map = sorted(symptom_id_map.items(), key=lambda x: len(x[1]), reverse=True)
# symptom_id_map = symptom_id_map[:20]
#
# with open("symptom_tweet_map_2.txt", 'w', encoding='utf-8') as f:
#     json.dump(symptom_id_map, f)

# with open("symptom_tweet_map_2.txt", 'r', encoding='utf-8') as f:
# symptom_id_map = json.loads(f.read())

# symptoms_with_sample_text = []
# for symptom_and_ids in symptom_id_map:
# row = [symptom_and_ids[0], len(symptom_and_ids[1])]
# for tweet_id in symptom_and_ids[1][:10]:
# tweet_id = tweet_id[0]
# tweet_text = client.tweet_id_to_text(tweet_id)
# row.append(tweet_text)
# symptoms_with_sample_text.append(row)

# with open("symptom_tweet_map_3.txt", 'w', encoding='utf-8') as f:
# writer = csv.writer(f)
# writer.writerows(symptoms_with_sample_text)


def read_lexical_map():
    with open("lexical_map.txt", 'r', encoding='utf-8') as f:
        lexical_map = json.loads(f.read())
    return lexical_map


def read_symptoms():
    with open('symptom_words.txt', 'r', encoding='utf-8') as f:
        symptoms = f.read()
    symptoms = symptoms.split("\n")
    return symptoms


def concat_all_symptoms():
    symptoms_1 = "other_data/COVID-Twitter-Symptom-Lexicon.tsv"
    symptoms_2 = "other_data/expanded_covid_symptom_exps.txt"
    symptoms_3 = "other_data/COVID-Reddit-Long-Symptom-Lexicon-Unique.tsv"

    symptoms = []
    with open(symptoms_1) as f:
        csv_file = csv.reader(f, delimiter="\t", quotechar='"')
        for row in csv_file:
            symptoms.append(row[0])
            symptoms.append(row[2])

    with open(symptoms_3) as f:
        csv_file = csv.reader(f, delimiter="\t", quotechar='"')
        for row in csv_file:
            symptoms.append(row[0])
            symptoms.append(row[2])

    with open(symptoms_2) as f:
        lines = f.readlines()
        lines = [line.strip() for line in lines]
        symptoms += lines

    symptom_words = []
    for symptom in symptoms:
        words = symptom.split(" ")
        symptom_words += words
    symptom_words = [s.lower() for s in symptom_words]
    symptom_words = list(set(symptom_words))
    symptom_words = sorted(symptom_words, key=len)
    with open('symptom_words.txt', 'w', encoding='utf-8') as f:
        f.write("\n".join(symptom_words))


if __name__ == "__main__":
    main()
