from config import LOCAL_DB_CREDENTIALS
from db.db_client import PsqlClient, establish_psql_connection
from db.db_add_vaccine_mentions import update_vaccine_mentions


def main():
    update_imported_data()


def update_imported_data():
    conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    client = PsqlClient(conn)
    # ts and associated index are auto-updated when new tweets are added. 
    update_vaccine_mentions(client)
    client.create_vaccine_tweets_table()
    
    ## TODO: run this once you get back to misinfo analysis
    cur = conn.cursor()
    cur.execute('''UPDATE links SET full_url_trimmed = LEFT(full_url,1000)
               WHERE full_url_trimmed IS NULL''')
    conn.commit()
    cur.close()
    client.create_misinfo_tweets_table()
    
    

if __name__ == "__main__":
    main()
