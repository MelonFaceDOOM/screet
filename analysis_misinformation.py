import pandas as pd
import re
import datetime
import plotly.express as px
from config import LOCAL_DB_CREDENTIALS
from utilities import minimal_url, extract_domain
from db.db_client import PsqlClient, establish_psql_connection
from misinfo.misinfo_lists import fc_articles_to_exclude, search_terms


pd.options.mode.chained_assignment = None  # default='warn'


def main():
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    client = PsqlClient(conn)
    tweets = client.get_all_misinfo_tweets()
    tweets = filter_out_excluded_fc_articles(tweets)
    df = pd.DataFrame(tweets)
    df['domain'] = df.apply(lambda x: extract_domain(x['true_source_link_trimmed']), axis=1)
    df.to_csv('all_misinfo.csv')
    # client.create_misinfo_tweets_table()
    # search_term_analysis(client)
    # total_misinfo_analysis(client)


def search_term_analysis(client):
    search_terms_and_results = search_misinfo_tweets_for_search_terms(search_terms)
    # search_terms_and_results.sort(key=lambda x: len(x[1]), reverse=True)
    search_term_dfs = []
    for search_term_and_results in search_terms_and_results:
        if len(search_term_and_results[1]) > 0:
            filtered_results = filter_out_excluded_fc_articles(search_term_and_results[1])
            mt = MisinfoTweets(search_term_and_results[0], filtered_results)
            search_term_dfs.append(mt.daily_retweet_df.rename({'retweet_count': mt.title}, axis=1)[[mt.title]])
    search_term_dfs.sort(key=lambda x: len(x), reverse=True)
    fig = create_misinfo_search_daily_retweet_line_graph(search_term_dfs[:8])
    fig.show()


def total_misinfo_analysis(client):
    misinfo_tweets = client.get_all_misinfo_tweets()
    misinfo_tweets = filter_out_excluded_fc_articles(misinfo_tweets)
    mt = MisinfoTweets('total', misinfo_tweets)
    mt.troubleshoot_df = mt.troubleshoot_df.sort_values(by=['tsl_rt'], ascending=False)
    # for index, row in mt.troubleshoot_df.head(20).iterrows():
    #     print(row['article_link'], row['article_source_link'])
    fig = create_daily_retweet_fig(mt.daily_retweet_df[['retweet_count']])
    peak_tweets = mt.get_tweets_for_peaks()
    add_peak_labels_to_daily_retweet_fig(mt.daily_retweet_df, fig, peak_tweets)
    fig.show()
    # print(mt.daily_retweet_df.loc['2021-01-01'])
    for peak_tweet in peak_tweets:
        ts_rt_on_day = mt.daily_true_source_retweet_df.loc[peak_tweet[0].date()]
        retweets_on_the_day = ts_rt_on_day['retweet_count'].sum()
        this_ts_retweets = ts_rt_on_day[ts_rt_on_day['true_source_link_trimmed'] == peak_tweet[1]['true_source_link_trimmed']]['retweet_count'][0]
        proportion_on_the_day = this_ts_retweets / retweets_on_the_day

        print(peak_tweet[0])
        print(this_ts_retweets)
        print(proportion_on_the_day)
        print(peak_tweet[1]['claim'])


def search_misinfo_tweets_for_search_terms(search_terms):
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    client = PsqlClient(conn)
    search_terms_and_results = []
    for search_term in search_terms:
        results = client.search_misinfo_tweets_claim_text(search_term)
        search_terms_and_results.append([search_term, results])
    conn.close()
    return search_terms_and_results


def create_daily_retweet_fig(df):
    fig = px.line(df, x=df.index,
                  y=df.columns, title='misinfo', labels={
        "value": "Total retweets"})
    return fig


def add_peak_labels_to_daily_retweet_fig(df, fig, peak_tweets):
    i = 1
    for date, top_tweet in peak_tweets:
        # print(i, date, top_tweet['claim'])
        volume_on_date = df.loc[date]['retweet_count']
        # print(f"{top_tweet['retweet_count']/volume_on_date}\t{top_tweet['claim']}")
        # print(retweet_df.loc[date]['volume'])
        fig.add_annotation(x=date, y=volume_on_date,
                           text=i,
                           showarrow=False,
                           yshift=10)
        i += 1
    return fig


def create_misinfo_search_daily_retweet_line_graph(search_term_dataframes):
    df_0 = search_term_dataframes.pop(0)
    for df in search_term_dataframes:
        df_0 = df_0.join(df, how='outer')
    fig = px.line(df_0, x=df_0.index, y=df_0.columns, title='misinfo', labels={
        "value": "Total retweets",
        "variable": "search term"
    })
    return fig


class MisinfoTweets:
    def __init__(self, title, tweets):
        self.title = title
        self.df = pd.DataFrame(tweets)
        self.clean_claim()
        self.filter_out_true()
        self.add_1_to_rt()
        self.daily_retweet_df = self.create_daily_retweet_df()
        self.daily_true_source_retweet_df = self.create_daily_true_source_retweet_df()
        self.troubleshoot_df = self.create_troubleshoot_df()
        self.add_peaks_to_daily_retweet_df()
        self.peaks_df = self.create_peaks_df(min_value=2000)
        self.peak_tweets = self.get_tweets_for_peaks()

    def filter_out_true(self):
        self.df = self.df[self.df['rating'] != 'TRUE']

    def clean_claim(self):
        self.df['claim'] = self.df.apply(lambda x: x['claim'].strip(), axis=1)

    def add_1_to_rt(self):
        # add one so that the source tweet is counted when summing
        self.df['retweet_count'] = self.df['retweet_count'] + 1

    def create_daily_retweet_df(self):
        # daily_retweet_df = self.df[['claim', 'rating', 'created_at', 'retweet_count']]
        # daily_retweet_df = daily_retweet_df.groupby(['created_at', 'claim']).sum()
        # daily_retweet_df = daily_retweet_df.reset_index()
        # daily_retweet_df = daily_retweet_df.loc[daily_retweet_df.index.repeat(daily_retweet_df.retweet_count)]
        # daily_retweet_df = daily_retweet_df.groupby('created_at').count()
        # daily_retweet_df = daily_retweet_df.reset_index(level=[1])
        daily_retweet_df = self.df[['created_at', 'retweet_count']]
        daily_retweet_df = daily_retweet_df.groupby('created_at').sum()
        daily_retweet_df.index = pd.to_datetime(daily_retweet_df.index)
        return daily_retweet_df

    def create_daily_true_source_retweet_df(self):
        daily_true_source_retweet_df = self.df[['true_source_link_trimmed', 'created_at', 'retweet_count']]
        daily_true_source_retweet_df = daily_true_source_retweet_df.groupby(['created_at', 'true_source_link_trimmed']).sum()
        daily_true_source_retweet_df = daily_true_source_retweet_df.reset_index(level=[1])
        return daily_true_source_retweet_df

    def create_troubleshoot_df(self):
        al_rt_df = self.df[['article_link', 'retweet_count']]
        al_rt_df['retweet_count'] = al_rt_df['retweet_count'] + 1  # add one for the actual tweet
        al_rt_df = al_rt_df.groupby('article_link').sum()
        al_sl_tsl_df = self.df[['article_link', 'article_source_link', 'true_source_link_trimmed']]
        al_sl_tsl_df = al_sl_tsl_df.drop_duplicates()
        al_sl_tsl_df = al_rt_df.merge(al_sl_tsl_df, how='inner', left_index=True, right_on='article_link')
        al_sl_tsl_df.rename({'retweet_count': 'al_rt'}, axis=1, inplace=True)

        tsl_rt_df = self.df[['true_source_link_trimmed', 'retweet_count']]
        tsl_rt_df['retweet_count'] = tsl_rt_df['retweet_count'] + 1  # add one for the actual tweet
        tsl_rt_df = tsl_rt_df.groupby('true_source_link_trimmed').sum()
        tsl_rt_df.rename({'retweet_count': 'tsl_rt'}, axis=1, inplace=True)
        al_sl_tsl_df = al_sl_tsl_df.merge(tsl_rt_df, how='inner', left_on='true_source_link_trimmed', right_index=True)
        return al_sl_tsl_df

    def get_top_tweets_from_day(self, date, n):
        temp_df = self.df[self.df['created_at'] == date.date()]
        temp_df = temp_df.sort_values(by=['retweet_count'], ascending=False)
        # return misinfo_df.loc[misinfo_df['retweet_count'].idxmax()]
        return temp_df.head(n)

    def add_peaks_to_daily_retweet_df(self):
        peak_column = []
        for i in range(0, len(self.daily_retweet_df)):
            peak_column.append(is_peak(i, self.daily_retweet_df))
        self.daily_retweet_df['peak'] = peak_column

    def create_peaks_df(self, min_value):
        peaks_df = self.daily_retweet_df[self.daily_retweet_df['peak'] == True]
        peaks_df = peaks_df[peaks_df['retweet_count'] >= min_value]
        return peaks_df

    def get_tweets_for_peaks(self):
        peak_tweets = []
        for value in self.peaks_df.index:
            top_tweet = self.get_top_tweets_from_day(value, 1)
            for index, t in top_tweet.iterrows():
                peak_tweets.append((value, t))
        return peak_tweets


def is_excluded(url, fc_articles_to_exclude):
    url = minimal_url(url)
    for e_url in fc_articles_to_exclude:
        e_url = minimal_url(e_url)
        if url == e_url:
            return True
    return False


def filter_out_excluded_fc_articles(misinfo_tweets):
    misinfo_tweets = [tweet for tweet in misinfo_tweets if
                      not is_excluded(tweet['article_link'], fc_articles_to_exclude)]
    return misinfo_tweets


def is_peak(iloc_value, df):
    try:
        previous_value = df.iloc[iloc_value - 1]['retweet_count']
    except:
        previous_value = 0
    current_value = df.iloc[iloc_value]['retweet_count']
    try:
        next_value = df.iloc[iloc_value + 1]['retweet_count']
    except:
        next_value = 0
    if current_value > previous_value and current_value > next_value:
        return True
    else:
        return False


def save_words(conn):
    cur = conn.cursor()
    cur.execute(
        '''SELECT claim FROM fact_checker_articles WHERE length(article_html)>1000 AND length(true_source_link_trimmed) <5''')
    results = cur.fetchall()
    all_words = []
    for r in results:
        sentence = re.sub(r'[^A-Za-z0-9 ]+', '', r[0])  # keep alphanumeric and spaces
        sentence = sentence.lower()
        words = sentence.split(' ')
        words = [w for w in words if len(w) > 2]
        all_words += words
    unique_words = sorted(list(set(all_words)))
    out_text = "\n".join([w for w in unique_words])
    with open('claim_words_where_source_available.txt', 'w', encoding='utf-8') as f:
        f.write(out_text)
    cur.close()


def save_lexemes(conn):
    cur = conn.cursor()
    cur.execute(
        '''SELECT claim FROM fact_checker_articles WHERE length(article_html)>1000 AND length(true_source_link_trimmed) <5''')
    results = cur.fetchall()
    all_words = []
    for r in results:
        search_query = r[0].replace('"', '')
        search_query = search_query.replace("'", "")
        cur.execute(f'''SELECT plainto_tsquery('english', '{search_query}')''')
        try:
            lexemes = cur.fetchone()[0]
        except:
            lexemes = None
        if lexemes:
            lexemes = lexemes.split('&')
            lexemes = [l.replace("'", "") for l in lexemes]
            lexemes = [l.replace(" ", "") for l in lexemes]
            lexemes = [l.replace(":", "") for l in lexemes]
            all_words += lexemes
    unique_words = sorted(list(set(all_words)))
    out_text = "\n".join([w for w in unique_words])
    with open('claim_words_where_source_available.txt', 'w', encoding='utf-8') as f:
        f.write(out_text)
    cur.close()


def get_count_for_each_word(conn):
    client = PsqlClient(conn=conn)
    client.create_misinfo_tweets_table()
    with open('claim_words_where_source_available.txt', 'r', encoding='utf-8') as f:
        words = f.read().split('\n')
    words_with_counts = []
    for word in words:
        try:
            results = client.search_misinfo_tweets_claim_text(word)
        except:
            results = None
        if results:
            words_with_counts.append([word, len(results)])
    words_with_counts.sort(key=lambda x: x[1])
    with open('words_with_count.txt', 'w', encoding='utf-8') as f:
        for l in words_with_counts:
            f.write(l[0] + ',' + str(l[1]) + '\n')


if __name__ == "__main__":
    main()
