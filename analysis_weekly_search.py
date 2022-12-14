import re
import os
import datetime
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
from psycopg2.extras import RealDictCursor
from wordcloud import WordCloud

from config import LOCAL_DB_CREDENTIALS
from db.db_client import establish_psql_connection
from analysis_find_aefis import get_all_unique_aefis_with_date_for_data_source


data_sources = ["monkeypox", "polio"]

def main():
    do_todays_analysis()
    # tweets_for_data_source = get_tweets_for_data_source('polio')
    # df = pd.DataFrame(tweets_for_data_source)
    # df.hist(column='date_created')
        
def do_todays_analysis():
    date = datetime.date.today()
    output_folder_path = f"analysis/{' '.join(data_sources)} {date}"
    for data_source in data_sources:
        data_file_path, aefi_file_path = make_folder_and_set_filepaths(output_folder_path=output_folder_path,
                                                                       data_source=data_source,
                                                                       date=date)
        query_and_save_data(data_file_path=data_file_path,
                            aefi_file_path=aefi_file_path,
                            data_source=data_source)
                         
        df = pd.read_csv(data_file_path, encoding='utf-8')
        df = filter_out_old_data(df)
        df_aefi = pd.read_csv(aefi_file_path, encoding='utf-8')
        df_aefi = filter_out_old_data(df_aefi)

        dates = df['created_at'].unique()
        dates_and_counts = get_counts_for_dfs_on_dates(
            dates, dfs_and_titles=[[df, f'{data_source} Tweets'],
                        [df_aefi, f'Potential {data_source} Vaccine AEFIs']]
            )
        df_dates_and_counts = pd.DataFrame(dates_and_counts, columns=['Date', 'Tweet Classification', 'Tweet Count'])
        
        tweets_by_date = create_fig_tweets_and_aefis_by_date(df_dates_and_counts)
        tweets_by_date_file_path = os.path.join(output_folder_path, f"{data_source}_{date}_tweets_by_date.png")    
        tweets_by_date.write_image(tweets_by_date_file_path, format="png", width=1800, height=1200)
        
        create_wordcloud(df_aefi) # creates the figure in the plt object
        wordcloud_file_path = os.path.join(output_folder_path, f"{data_source}_{date}_wordcloud.png")    
        plt.savefig(wordcloud_file_path)
        

def make_folder_and_set_filepaths(output_folder_path, data_source, date):
    os.makedirs(output_folder_path, exist_ok=True)
    output_file_name = f"{data_source}_{date}.csv"
    output_file_path = os.path.join(output_folder_path, output_file_name)    
    aefi_output_file_name = f"{data_source}_aefis_{date}.csv"
    aefi_output_file_path = os.path.join(output_folder_path, aefi_output_file_name)
    return output_file_path, aefi_output_file_path
    
    
def query_and_save_data(data_file_path, aefi_file_path, data_source):
    # saving the data to csv will save time if we re-run analysis on the data because the query takes a while.
    tweets_for_data_source = get_tweets_for_data_source(data_source)
    for tweet in tweets_for_data_source:
         # todo: this should be temporary. fix by changing the values in the db (but that will take long to run).
        tweet['tweet_text'] = tweet['tweet_text'].replace(u'\xa0', u' ')
        tweet['tweet_text'] = tweet['tweet_text'].replace('\n', '/n')
        tweet['tweet_text'] = tweet['tweet_text'].replace('\r', '/r')
            
    df = pd.DataFrame(tweets_for_data_source)
    df.to_csv(data_file_path, encoding='utf-8', index=False)
    unique_aefi_text_and_date = get_all_unique_aefis_with_date_for_data_source(data_source)
    df_aefi = pd.DataFrame(unique_aefi_text_and_date, columns=['tweet_text', 'created_at'])
    df_aefi.to_csv(aefi_file_path, encoding='utf-8', index=False)
    

def get_tweets_for_data_source(data_source):
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute('''SELECT * FROM tweets WHERE source = %s''', (data_source,))
    r = cur.fetchall()
    return r
    
    
def filter_out_old_data(df):
    df['created_at']= pd.to_datetime(df['created_at'])
    df = df[df['created_at'] >= "2022-08-01"]
    return df
    

def get_counts_for_dfs_on_dates(dates, dfs_and_titles=[]):
    # dfs_and_titles is a list of pairs: [[df, df_title], [df2, df2_title], etc.]
    # dates_and_counts is a list of triples: [[date, title, count], etc]. It is in this format so that it can be easily passed to plotly to create a graph
    dates_and_counts = []
    for date in dates:
        date_counts = []
        for df, title in dfs_and_titles:
            df_count_on_day = len(df[df['created_at'] == date])
            dates_and_counts.append([date, title, df_count_on_day])
    return dates_and_counts


def create_fig_tweets_and_aefis_by_date(df_dates_and_counts):
    fig = px.bar(df_dates_and_counts, x='Date', y='Tweet Count', text='Tweet Count', color='Tweet Classification', barmode='overlay')
    fig.update_xaxes(title_font=dict(size=24), tickfont=dict(size=24))
    fig.update_yaxes(title_font=dict(size=24), tickfont=dict(size=24))
    fig.update_traces(texttemplate='%{text:,}')
    fig.update_traces(textposition='outside')
    fig.update_layout(uniformtext_minsize=22, uniformtext_mode='show')
    fig.update_layout(legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99,
                      font=dict(size=22)))
    return fig
    
        
def clean_text(tweet_text):
    tweet_text = tweet_text.lower()
    tweet_text = tweet_text.replace('&amp;', '&')
    tweet_text = tweet_text.replace('.', ' ')
    tweet_text = tweet_text.replace(',', ' ')
    tweet_text = tweet_text.replace(';', ' ')
    tweet_text = tweet_text.replace("'", '')
    tweet_text = re.sub("(https:\/\/t.?co\/[\w]+)", "",tweet_text)
    tweet_text = tweet_text.strip(' \n')
    tweet_text = tweet_text.split(' ')
    tweet_words = []
    for word in tweet_text:
        if len(word) > 2:
            tweet_words.append(word)
    tweet_text = " ".join(tweet_words)
    return tweet_text
    
    
def create_wordcloud(df):
    cleaned_text = df.apply(lambda x: clean_text(x['tweet_text']), axis=1)
    concat_text = cleaned_text.str.cat()
    wordcloud = WordCloud(max_words=50, width=1000, height=600, min_word_length=3).generate(concat_text)
    plt.figure(figsize=(20,12))
    plt.tight_layout(pad=0)
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    
    
if __name__ == "__main__":
    main()
