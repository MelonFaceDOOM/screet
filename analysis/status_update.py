import datetime
import calendar
import pandas as pd
import plotly.express as px
from dateutil import rrule
from utilities import get_file_paths_from_folder

from config import LOCAL_DB_CREDENTIALS
from symptoms.process_labeled_aefis import get_symptoms, format_query
from analysis.find_misinformation import filter_out_excluded_fc_articles
from db.db_client import PsqlClient, establish_psql_connection

def main():
    do_status_update()


def do_status_update():
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    # get_aefi_month_counts(conn)
    
    # counts = get_vaxx_month_counts(conn)
    # for c in counts:
        # print(c)
        
    # get_misinfo_month_counts(conn)
    
    cur = conn.cursor()
    # cur.execute('''REINDEX TABLE tweets''')
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_tweets_date ON tweets(created_at)''')
    conn.commit()
    months_and_counts = get_month_counts(conn)
    for i in months_and_counts:
        print(i)
    

    
    
    # cur = conn.cursor()
    # cur.execute('''SELECT COUNT(*) FROM tweets WHERE created_at > '2020-12-31'::date''')
    # r = cur.fetchall()
    # print(r)
    
    
    
    # link_count = get_link_count(conn)
    # print(link_count)
    # fc_article_count = get_fc_article_count(conn)
    # print(fc_article_count)
    # tsl_article_count = get_fc_article_with_true_source(conn)
    # print(tsl_article_count)
    
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
    
def get_month_counts(conn):
    cur = conn.cursor()
    months_and_counts = []
    
    start_date = datetime.datetime(2020, 1, 1)
    end_date = datetime.datetime(2022, 4, 30)
     
    for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_date, until=end_date):
        print(dt)
        sql = f'''SELECT COUNT(*) FROM tweets WHERE created_at >= '{dt.date()}'::date
                   AND created_at < '{add_one_month(dt).date()}'::date'''
        cur.execute(sql)
        result = cur.fetchone()[0]
        months_and_counts.append([dt.date(), result])
    cur.close()
    return months_and_counts
    
    
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
    df = pd.DataFrame({'title': segments_titles, 'amount': x_data, 'grouping':grouping},
                      columns=['title', 'amount', 'grouping'])
    fig = px.bar(df, x="amount", y='grouping', color='title', height=400, title=title, orientation='h')
    annotations = []
    bar_start_pos = 0
    for idx, x in enumerate(x_data):
        label_x_pos = bar_start_pos + x/2
        bar_start_pos += x
        label_text =  str(x) + "<br><br>" + segments_titles[idx]
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


    
if __name__ == "__main__":
    main()
