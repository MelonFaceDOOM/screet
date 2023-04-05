import sys
from analysis.find_aefis import save_aefis_for_all_data_sources
from analysis.status_update import do_status_update
from analysis.find_misinformation import save_all_misinfo
from analysis.search_in_tweet_db import create_hist_for_search_term


def main():
    command = sys.argv[1].lower().strip()
    if command == "aefis":
        save_aefis_for_all_data_sources()
    elif command == "status":
        do_status_update()
    elif command == 'misinfo':
        save_all_misinfo()
    elif command == 'search':
        try:
            search_terms = sys.argv[2:]
        except IndexError:
            raise ValueError('must provide at least one search term')
        search_terms = [s.strip().lower() for s in search_terms]
        create_hist_for_search_term(search_terms)
    else:
        raise ValueError(f"command {command} not recognized")


if __name__ == "__main__":
    main()
