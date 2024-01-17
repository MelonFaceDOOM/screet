import sys
import pandas as pd
from datetime import datetime
from config import LOCAL_DB_CREDENTIALS
from db.db_client import PsqlClient, establish_psql_connection
from symptoms import manage_symptoms
from symptoms.QA_symptoms import SymptomQA


def main():
    command = sys.argv[1].lower().strip()
    if command == "create_queries":
        conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
        client = PsqlClient(conn=conn)
        manage_symptoms.create_symptom_queries_file(client)
    elif command == "find":
        data_source = sys.argv[2].lower().strip()
        if data_source == "all":
            data_sources = ['panacea', 'polio', 'monkeypox', 'pertussis']
        else:
            data_sources = [data_source]
        for data_source in data_sources:
            conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
            sqa = SymptomQA(conn=conn)
            tweets = sqa.search_for_querylist(data_source)
            today = datetime.today().strftime('%Y-%m-%d')
            df = pd.DataFrame(tweets)
            df.to_csv(f'symptoms/sv_{data_source}_{today}.csv', index=False)
    else:
        raise ValueError(f"command {command} not recognized")


if __name__ == "__main__":
    main()
