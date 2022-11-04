import os
from utilities import get_file_paths_from_folder
from db_client import PsqlClient, establish_psql_connection
from config import LOCAL_DB_CREDENTIALS
from dehydrated.panacea_create_samples import keep_files_in_date_range
from psycopg2.extras import execute_values


def main():
    # insert_all_panacea_ids_into_temp_table()
    # save_missing_ids_to_file()


def print_basic_info():
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    cur = conn.cursor()
    cur.execute('''SELECT count(*) FROM tweets''')
    results = cur.fetchone()
    print(results)
    cur.execute('''SELECT count(*) FROM users''')
    results = cur.fetchone()
    print(results)
    cur.execute('''SELECT count(*) FROM links''')
    results = cur.fetchone()
    print(results)
    

def save_missing_ids_to_file():
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    cur = conn.cursor()
    cur.execute('''SELECT A.id FROM temp_tweet_ids A
                   LEFT JOIN tweets B
                   ON A.id = B.id
                   WHERE B.id IS NULL
                   ''')
    results = cur.fetchall()
    results = [str(r[0]) for r in results]
    results = "\n".join(results)
    with open('missing_ids.txt', 'w') as f:
        f.write(results)
    cur.close()
    conn.close()
    

def insert_all_panacea_ids_into_temp_table():
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    cur = conn.cursor()
    cur.execute('''DROP TABLE IF EXISTS temp_tweet_ids''')
    cur.execute('''CREATE TABLE IF NOT EXISTS temp_tweet_ids
                    (id BIGINT PRIMARY KEY NOT NULL)''')
    conn.commit()
    dehydrated_id_files = get_file_paths_from_folder(r'C:\proj\covid_scraping\twitter\dehydrated\panacea\ids')
    dehydrated_id_files = keep_files_in_date_range(dehydrated_id_files)
    for file in dehydrated_id_files:
        print(file)
        with open(file, 'r') as f:
            id_list = f.read()
        id_list = id_list.split('\n')
        id_list = [int(tweet_id) for tweet_id in id_list]
        save_many_tweet_ids(conn, [id_list])
    cur.execute('''SELECT count(*) FROM temp_tweet_ids''')
    results = cur.fetchone()
    cur.close()
    conn.close()
    print(results)
    
def save_many_tweet_ids(conn, ids):
    cur = conn.cursor()
    cur.execute("insert into temp_tweet_ids (id) select unnest (array [%s]) ON CONFLICT DO NOTHING", ids)
    # ids = [(i,) for i in ids]
    # query = f"INSERT INTO temp_tweet_ids (id) VALUES %s ON CONFLICT DO NOTHING"
    # execute_values(cur, query, ids)
    conn.commit()
    cur.close()
    
    
    
    
    # tweet_ids_from_db = client.get_all_tweet_ids()
    # source_ids = []
    # for id_chunk in source_dehydrated_id_chunks():
        # source_ids += id_chunk
    # sample_ids = []
    # for id_chunk in sample_dehydrated_id_chunks():
        # sample_ids += id_chunk
    # ids_in_samples_but_not_in_db = list(set(sample_ids) - set(tweet_ids_from_db))
    # with open('missing_from_db.txt') as f:
        # for sample_id in ids_in_samples_but_not_in_db:
            # f.write(f'{str(sample_id)}\n')
    # ids_in_source_but_not_in_samples = list(set(source_ids) - set(sample_ids))
    # with open('missing_from_samples.txt') as f:
        # for sourced_id in ids_in_source_but_not_in_samples:
            # f.write(f'{str(sourced_id)}\n')


def source_dehydrated_id_chunks():
    file_paths = get_file_paths_from_folder(config.PANACEA_SOURCE_IDS_FOLDER)
    file_paths = keep_files_in_date_range(file_paths)
    for file_path in file_paths:
        with open(file_path, 'r', encoding='utf-8') as f:
            id_list = f.read()
        id_list = id_list.split('\n')
        id_list = [int(_id) for _id in id_list]
        yield id_list


def sample_dehydrated_id_chunks():
    file_paths = get_file_paths_from_folder(config.PANACEA_DEHYDRATED_SAMPLE_FOLDER)
    for file_path in file_paths:
        with open(file_path, 'r', encoding='utf-8') as f:
            id_list = f.read()
        id_list = id_list.split('\n')
        id_list = [int(_id) for _id in id_list]
        yield id_list


if __name__ == "__main__":
    main()
