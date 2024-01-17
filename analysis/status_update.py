import datetime
import calendar
import json
import pandas
import plotly.express as px
from dateutil import rrule
import pandas as pd
import csv
from psycopg2.extras import RealDictCursor
from config import LOCAL_DB_CREDENTIALS
from utilities import get_file_paths_from_folder
from analysis.find_misinformation import filter_out_excluded_fc_articles
from db.db_client import PsqlClient, establish_psql_connection
from symptoms.QA_symptoms import SymptomQA
from symptoms.manage_symptoms import symptoms_to_queries

"""total reddit submissions by month

for 3 sources:
tweet count by month 
tweet count by month where symptom found for 3 sources
tweet count by month where personal report found

two main questions:
Can social media data show vaccine-associated symptoms before they show up through traditional reporting
How much social media volume is needed for this to be useful? (covid vs polio/mp) 

word cloud
sentiment analysis
sample tweets


TODO: rename panacea to covid
"""

data_sources = ['chickenpox', 'cholera', 'diphtheria', 'encephalitis', 'haemophilus influenzae disease',
                'hepatitis', 'human papillomavirus', 'influenza', 'invasive meningococcal disease',
                'invasive pneumococcal disease', 'measles', 'monkeypox',
                'mumps', 'panacea', 'pertussis', 'polio', 'rabies', 'rotavirus', 'rubella', 'shingles', 'smallpox',
                'tetanus', 'tuberculosis', 'typhoid', 'varicella', 'yellow fever']

def main():
    # client = PsqlClient(conn)
    # client.make_indices()
    # get_month_counts(conn, data_source="panacea")
    do_status_update()


def do_status_update():
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    create_monthly_count_csv(conn)
    create_monthly_vs_count_csv(conn)
    get_top_symptoms(conn)


def create_monthly_count_csv(conn):
    # get today's date so we can put in file names
    today = datetime.date.today().strftime("%Y-%m-%d")
    monthly_tweet_counts = get_monthly_tweet_counts(conn)
    df = pandas.DataFrame(monthly_tweet_counts)
    df.to_csv(f'monthly_tweet_counts_{today}.csv', index=False)


def create_monthly_vs_count_csv(conn):
    # get today's date so we can put in file names
    today = datetime.date.today().strftime("%Y-%m-%d")
    months_and_counts_vs_dfs = []
    for data_source in data_sources:
        vs_df = get_vaccine_symptom_month_counts(conn, data_source)
        vs_df['Disease'] = data_source
        months_and_counts_vs_dfs.append(vs_df)
    # concat months_and_counts_vs_dfs and save
    combined_vs_df = pd.concat(months_and_counts_vs_dfs, ignore_index=True)
    combined_vs_df.to_csv(f'monthly_vs_tweet_counts_{today}.csv', index=False)


def months_and_counts_to_df(months_and_counts):
    df = pd.DataFrame(months_and_counts)
    df = df.melt(id_vars='disease', var_name='date', value_name='amount')
    return df


def get_data_sources(conn):
    """this takes like 10 minutes to run"""
    cur = conn.cursor()
    cur.execute('''SELECT DISTINCT source FROM tweets''')
    data_sources = cur.fetchall()
    data_sources = [i[0] for i in data_sources]
    return data_sources


def get_monthly_tweet_counts(conn):
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute('''SELECT DATE_TRUNC('month', created_at) AS month, source, COUNT(*) AS count
                   FROM tweets
                   GROUP BY DATE_TRUNC('month', created_at), source;''')
    months_and_counts = cur.fetchall()
    cleaned = []
    for i in months_and_counts:
        row = {}
        row['Month'] = i['month'].strftime('%Y-%m')
        row['Disease'] = i['source']
        row['Count'] = i['count']
        cleaned.append(row)
    cur.close()
    return cleaned


def get_top_symptoms(conn):
    """only use symptoms in dict for now. we'll figure out a way to classify the generated symptoms later and then
    we can just use the main queries file instead of rebuilding this half-list."""
    client = SymptomQA(conn)
    with open('symptoms/symptom_dict.txt', 'r') as f:
        symptom_dict = json.loads(f.read())
    symptoms_from_dict = [symptom_dict[key] for key in symptom_dict]
    symptoms_from_dict = [symptom for sublist in symptoms_from_dict for symptom in sublist]
    top_10_dfs = []
    for data_source in data_sources:
        df = pd.DataFrame(client.search_for_symptoms(symptoms_from_dict, data_source))
        counts = df['symptom'].value_counts().reset_index()
        counts.columns = ['Symptom', 'Count']
        top_10 = counts.head(10)
        top_10['Disease'] = data_source
        top_10_dfs.append(top_10)
    combined_df = pd.concat(top_10_dfs, ignore_index=True)
    combined_df.to_csv('top_symptoms_for_each_disease.csv', index=False)


def get_month_counts_old(conn, data_source, start_date=None, end_date=None):
    months_and_counts = {'disease': data_source}
    cur = conn.cursor()
    if not start_date:
        cur.execute('''SELECT min(created_at) FROM tweets WHERE source = %s''', (data_source,))
        start_date = cur.fetchone()[0]
        start_date = start_date.replace(day=1)
    if not end_date:
        cur.execute('''SELECT max(created_at) FROM tweets WHERE source = %s''', (data_source,))
        end_date = cur.fetchone()[0]
        end_date = end_date.replace(day=1)

    for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_date, until=end_date):
        sql = f'''SELECT COUNT(*) FROM tweets WHERE created_at >= '{dt.date()}'::date
                   AND created_at < '{add_one_month(dt).date()}'::date
                   AND source = %s'''
        cur.execute(sql, (data_source,))
        result = cur.fetchone()[0]
        months_and_counts[dt.strftime('%Y-%m')] = result
        # months_and_counts.append([dt.strftime('%Y-%m'), result])
    cur.close()
    return months_and_counts


def get_vaccine_symptom_month_counts(conn, data_source):
    sqa = SymptomQA(conn=conn)
    tweets = sqa.search_for_querylist(data_source=data_source)
    df = pd.DataFrame(tweets)
    df['created_at'] = pd.to_datetime(df['created_at'])
    df = df['created_at'].groupby(df['created_at'].dt.to_period("M")).agg('count')
    df = df.reset_index(name="Count")
    df = df.rename(columns={'created_at': 'Month'})
    return df
    # this is another method of doing the same grouping
    # df_2 = df.groupby(pd.Grouper(key='created_at', freq='M')).size().reset_index(name='count')


def get_misinfo_month_counts(conn):
    client = PsqlClient(conn)
    tweets = client.get_all_misinfo_tweets()
    tweets = filter_out_excluded_fc_articles(tweets)
    df = pd.DataFrame(tweets)
    df['month'] = df.apply(lambda x: convert_to_month(x['created_at']), axis=1)
    df = df.groupby(['month']).count()
    print(df.head(15))


def get_aefi_month_counts(conn):
    client = PsqlClient(conn)
    ids_and_dates = aefis(client)
    col_names = ['id', 'created_at']
    df = pd.DataFrame(ids_and_dates, columns=col_names)
    df = df.drop_duplicates(subset='id', keep="last")
    df['month'] = df.apply(lambda x: convert_to_month(x['created_at']), axis=1)
    df = df.groupby(['month']).count()
    print(df.head(15))


def convert_to_month(date):
    # date = date.replace(day=1)
    date = datetime.date(year=date.year, month=date.month, day=1)
    return date


def create_temp_vax_date_table(conn):
    cur = conn.cursor()
    cur.execute('''create TEMP TABLE vaccine_tweet_dates(id BIGINT, created_at DATE);''')
    cur.execute('''INSERT INTO vaccine_tweet_dates
                   SELECT B.id, B.created_at FROM vaccine_tweets A
                   INNER JOIN tweets B
                   ON A.id = B.id''')
    cur.close()


def get_vaxx_month_counts(conn):
    create_temp_vax_date_table(conn)
    cur = conn.cursor()
    months_and_counts = []
    start_date = datetime.datetime(2021, 1, 1)
    end_date = datetime.datetime(2022, 1, 1)

    for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_date, until=end_date):
        sql = f'''SELECT COUNT(*) FROM vaccine_tweet_dates
                       WHERE created_at >= '{dt.date()}'::date
                       AND created_at < '{add_one_month(dt).date()}'::date'''
        cur.execute(sql)
        result = cur.fetchone()[0]
        months_and_counts.append([dt.date(), result])
    cur.close()
    return months_and_counts


def add_one_month(orig_date):
    # advance year and month by one month
    new_year = orig_date.year
    new_month = orig_date.month + 1
    # note: in datetime.date, months go from 1 to 12
    if new_month > 12:
        new_year += 1
        new_month -= 12

    last_day_of_month = calendar.monthrange(new_year, new_month)[1]
    new_day = min(orig_date.day, last_day_of_month)

    return orig_date.replace(year=new_year, month=new_month, day=new_day)


def aefis(client):
    symptoms = get_symptoms()
    ids_and_dates = []
    for symptom in symptoms:
        for symptom_synonym in symptoms[symptom]:
            formatted_query = format_query(symptom_synonym)
            results = client.search_vaccine_tweet_text(formatted_query)
            symptom_id_and_dates = [[str(r['id']), r['created_at']] for r in results]
            ids_and_dates += symptom_id_and_dates
    return ids_and_dates


def test_chart():
    title = 'test'
    x_data = [1000, 500, 200]
    segments_titles = ['total', 's1', 's2']
    fig = create_single_horizontal_bar_chart(title=title, x_data=x_data, segments_titles=segments_titles)
    return fig


def aefi_chart():
    total_db_tweets = 94372763
    vaccine_match_count = 1999790
    symptoms_found_in_vaccine_tweets = 368095
    x_data = ['Tweets mentioning vaccine', 'Vaccine tweets with symptom', 'Manual labeling subset']
    y_data = [1999790, 368095, 25117]
    fig = create_vertical_bar_chart(y_data=y_data, x_data=x_data, title='AEFIs')
    fig.show()


def get_2020_count():
    dehydrated_id_files = get_file_paths_from_folder(r'C:\proj\covid_scraping\twitter\dehydrated\panacea\source_1')
    id_files_2020 = keep_2020_files(dehydrated_id_files)
    tweet_count_2020 = count_all_rows_in_files(id_files_2020)
    return tweet_count_2020


def get_2021_count():
    dehydrated_id_files = get_file_paths_from_folder(r'C:\proj\covid_scraping\twitter\dehydrated\panacea\source_1')
    id_files_2021 = keep_2021_files(dehydrated_id_files)
    tweet_count_2021 = count_all_rows_in_files(id_files_2021)
    return tweet_count_2021


def get_2022_count():
    dehydrated_id_files = get_file_paths_from_folder(r'C:\proj\covid_scraping\twitter\dehydrated\panacea\source_1')
    id_files_2022 = keep_2022_files(dehydrated_id_files)
    tweet_count_2022 = count_all_rows_in_files(id_files_2022)
    other_2022_files = get_file_paths_from_folder(r'C:\proj\covid_scraping\twitter\dehydrated\panacea\source_2')
    tweet_count_2022 += count_all_rows_in_files(other_2022_files)
    return tweet_count_2022


def create_vertical_bar_chart(y_data, x_data, title):
    df = pd.DataFrame({'title': title, 'count': y_data, 'category': x_data},
                      columns=['title', 'count', 'category'])
    fig = px.bar(df, x='category', y='count', color='title', height=400, title=title)
    return fig


def create_single_horizontal_bar_chart(title, x_data, segments_titles):
    grouping = [1 for x in x_data]
    df = pd.DataFrame({'title': segments_titles, 'amount': x_data, 'grouping': grouping},
                      columns=['title', 'amount', 'grouping'])
    fig = px.bar(df, x="amount", y='grouping', color='title', height=400, title=title, orientation='h')
    annotations = []
    bar_start_pos = 0
    for idx, x in enumerate(x_data):
        label_x_pos = bar_start_pos + x / 2
        bar_start_pos += x
        label_text = str(x) + "<br><br>" + segments_titles[idx]
        annotations.append(dict(xref='x', yref='y',
                                x=label_x_pos, y=1,
                                text=label_text,
                                font=dict(family='Arial', size=18,
                                          color='rgb(248, 248, 255)'),
                                showarrow=False))
    fig.update_layout(annotations=annotations)
    return fig


def wat():
    # symptom update

    total_db_tweets = get_db_tweet_count_without_errors(conn)
    vaccine_match_count = get_vaccine_match_count(conn)
    symptoms_found_in_vaccine_tweets = 368095

    # 2) four vertical bars
    # #total tweets
    # #vaccine tweets
    # #symptoms found in vaccine tweets
    # sample 20k for manual labeling

    # misinformation update
    link_count = get_link_count(conn)
    fc_article_count = get_fc_article_count(conn)
    tsl_article_count = get_fc_article_with_true_source(conn)

    # 3) 
    # #total tweets
    # #tweets with links (unique of id in link table)
    # #total fc articles
    # #total source links so far extracted
    # #matches within tweet links


def keep_2020_files(dehydrated_filepaths):
    files_to_keep = []
    for file_path in dehydrated_filepaths:
        file_name = file_path.split('\\')[-1]
        if file_is_in_2020(file_name):
            files_to_keep.append(file_path)
    return files_to_keep


def keep_2021_files(dehydrated_filepaths):
    files_to_keep = []
    for file_path in dehydrated_filepaths:
        file_name = file_path.split('\\')[-1]
        if file_is_in_2021(file_name):
            files_to_keep.append(file_path)
    return files_to_keep


def keep_2022_files(dehydrated_filepaths):
    files_to_keep = []
    for file_path in dehydrated_filepaths:
        file_name = file_path.split('\\')[-1]
        if file_is_in_2022(file_name):
            files_to_keep.append(file_path)
    return files_to_keep


def file_is_in_2020(file_name):
    date = file_name[:10]
    date = datetime.date.fromisoformat(date)
    max_date = datetime.date.fromisoformat("2021-01-01")
    if date <= max_date:
        return True


def file_is_in_2021(file_name):
    date = file_name[:10]
    date = datetime.date.fromisoformat(date)
    min_date = datetime.date.fromisoformat("2021-01-01")
    max_date = datetime.date.fromisoformat("2021-12-31")
    if date >= min_date and date <= max_date:
        return True
    else:
        return False


def file_is_in_2022(file_name):
    date = file_name[:10]
    date = datetime.date.fromisoformat(date)
    min_date = datetime.date.fromisoformat("2022-01-01")
    max_date = datetime.date.fromisoformat("2022-12-31")
    if date >= min_date and date <= max_date:
        return True
    else:
        return False


def count_all_rows_in_files(filepaths):
    linecount = 0
    for filepath in filepaths:
        with open(filepath, 'r') as f:
            filetext = f.read()
        linecount += filetext.count('\n')
    return linecount


def get_total_db_tweet_count(conn):
    cur = conn.cursor()
    cur.execute('''SELECT count(*) FROM tweets''')
    tweets_count = cur.fetchone()[0]
    cur.execute('''SELECT count(*) FROM errors''')
    errors_count = cur.fetchone()[0]
    cur.close()
    return tweets_count + errors_count


def get_db_tweet_count_without_errors(conn):
    cur = conn.cursor()
    cur.execute('''SELECT count(*) FROM tweets''')
    tweets_count = cur.fetchone()[0]
    cur.close()
    return tweets_count


def get_vaccine_match_count(conn):
    client = PsqlClient(conn)
    client.create_temp_vaccine_tweet_subset()
    cur = conn.cursor()
    cur.execute('''SELECT count(*) FROM vaccine_tweets''')
    vaccine_match_count = cur.fetchone()[0]
    cur.close()
    return vaccine_match_count


def get_link_count(conn):
    cur = conn.cursor()
    cur.execute('''SELECT count(*) FROM links''')
    links_count = cur.fetchone()[0]
    cur.close()
    return links_count


def get_fc_article_count(conn):
    cur = conn.cursor()
    cur.execute('''SELECT count(*) FROM fact_checker_articles''')
    fc_article_count = cur.fetchone()[0]
    cur.close()
    return fc_article_count


def get_fc_article_with_true_source(conn):
    cur = conn.cursor()
    cur.execute('''SELECT count(*) FROM fact_checker_articles WHERE length(true_source_link) > 5''')
    tsl_article_count = cur.fetchone()[0]
    cur.close()
    return tsl_article_count



def old_aefi_thing():
    get_aefi_month_counts(conn)

    counts = get_vaxx_month_counts(conn)
    for c in counts:
        print(c)

    get_misinfo_month_counts(conn)

    cur = conn.cursor()
    # cur.execute('''REINDEX TABLE tweets''')
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_tweets_date ON tweets(created_at)''')
    conn.commit()
    months_and_counts = get_monthly_tweet_counts(conn)
    for i in months_and_counts:
        print(i)

    cur = conn.cursor()
    cur.execute('''SELECT COUNT(*) FROM tweets WHERE created_at > '2020-12-31'::date''')
    r = cur.fetchall()
    print(r)

    link_count = get_link_count(conn)
    print(link_count)
    fc_article_count = get_fc_article_count(conn)
    print(fc_article_count)
    tsl_article_count = get_fc_article_with_true_source(conn)
    print(tsl_article_count)



if __name__ == "__main__":
    main()
