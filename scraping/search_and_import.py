from scraping.search import execute_search_config
from db.import_search_data import save_files_in_search_results_folder
from db.update_imported_data import update_imported_data
from analysis.analysis_weekly_search import do_todays_analysis


def main():
    do_weekly_search()


def do_weekly_search():
    execute_search_config()
    # save_files_in_search_results_folder()
    # update_imported_data()
    # do_todays_analysis()


if __name__ == "__main__":
    main()
