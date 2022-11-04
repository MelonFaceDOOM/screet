import pandas as pd
import re
from config import LOCAL_DB_CREDENTIALS
from db_client import PsqlClient, establish_psql_connection
import sys


def main():
    
    # conn = establish_psql_connection(**LOCAL_DB_CREDENTIALS)
    # df = pd.read_csv('temp.csv')
    # print(len(df))
    # df2['text'] = df2.apply(lambda x: remove_links(x['text']), axis=1)
    # df2 = df2.drop_duplicates(subset='text', keep="last")

    with open('full_text_links_removed.csv', 'r', encoding='utf-8') as f, open('full_text_duplicates_removed.csv','w',encoding='utf-8') as f_out:
        for line in f:
            seen = set()
            for line in f:
                if line not in seen: 
                    seen.add(line)
                    f_out.write(line)

def remove_links_and_strip():
    with open('full_text.csv', 'r', encoding='utf-8') as f, open('full_text_links_removed.csv','w',encoding='utf-8') as f_out:
        for line in f:
            line = re.sub("(https:\/\/t.co\/[\w]+)", "", line)
            line = line.strip()
            line.replace('\n','/n')
            if len(line)>3:
                line += '\n'
                f_out.write(line)
            


def remove_links(text):
    text = re.sub("(https:\/\/t.co\/[\w]+)", "", text)
    return text
    
    
if __name__ == "__main__":
    main()

