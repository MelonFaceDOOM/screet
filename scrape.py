import sys
from scraping.search_and_import import do_weekly_search
from scraping.hydrate_ids import hydrate_id_files
from data.dehydrated.panacea.panacea_download_data import download_and_process_panacea_ids


def main():
    command = sys.argv[1].lower().strip()
    if command == "ids":
        hydrate_id_files()
    elif command == "pan":
        download_and_process_panacea_ids()
    elif command == 'search':
        do_weekly_search() # includes db import
    else:
        raise ValueError(f"command {command} not recognized")


if __name__ == "__main__":
    main()
