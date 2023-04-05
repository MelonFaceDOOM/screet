import pandas as pd
import plotly.express as px
from config import LOCAL_DB_CREDENTIALS
from db.db_client import PsqlClient, establish_psql_connection


def main():

    # booster_hist = create_hist_for_search_term(client, ["booster"])
    # booster_hist.show()
    
    clinic_searches = [["clinic", "closed"], ["cant", "get", "vaccine"], ["clinic and delay"]]
    for cs in clinic_searches:
        fig = create_hist_for_search_term(cs)
        fig.show()


def get_results_for_search_terms(client, search_terms):
    results = client.search_words_in_tweets(search_terms)
    print(len(results))
    df = pd.DataFrame(results)
    return df


def create_hist_for_search_term(search_terms):
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    client = PsqlClient(conn)
    df = get_results_for_search_terms(client, search_terms)
    df = df[['created_at', 'id']]
    fig = px.histogram(df, x='created_at')
    query = " and ".join(search_terms)
    fig.update_layout(
        title=f"Search Results: '{query}'",
        bargap=0.2,
        xaxis_title="Tweet Date",
        yaxis_title="Tweet Count",
        font=dict(
            family="Courier New, monospace",
            size=18,
            color="RebeccaPurple"
        )
    )
    return fig


if __name__ == "__main__":
    main()
