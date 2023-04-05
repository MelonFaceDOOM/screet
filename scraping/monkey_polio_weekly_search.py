from scraping.search import search_for_term
from db.import_search_data import save_files_in_search_results_folder
from db.update_imported_data import update_imported_data
from analysis.analysis_weekly_search import do_todays_analysis


def main():
    do_weekly_search()


def do_weekly_search():
    search_terms = ["polio", "monkeypox"]
    for search_term in search_terms:
        search_for_term(search_term)
    save_files_in_search_results_folder()
    update_imported_data()
    do_todays_analysis()


if __name__ == "__main__":
    main()