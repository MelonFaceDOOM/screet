import sys
from scraping.monkey_polio_weekly_search import do_weekly_search
from scraping.hydrate_ids import hydrate_id_files
from scraping.search import search_for_term


def main():
    command = sys.argv[1].lower().strip()
    if command == "ids":
        hydrate_id_files()
    elif command == "search_for_term":
        search_term = sys.argv[2].lower().strip()
        search_for_term(search_term)
    elif command == 'mp':
        do_weekly_search()
    else:
        raise ValueError(f"command {command} not recognized")


if __name__ == "__main__":
    main()
