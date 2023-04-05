import sys
from db.import_hydrated_data import import_all_panacea_data
from db.import_search_data import save_files_in_search_results_folder
from db.update_imported_data import update_imported_data


def main():
    command = sys.argv[1].lower().strip()
    if command == "import":
        command2 = sys.argv[2].lower().strip()
        if command2 == "panacea":
            import_all_panacea_data()
        elif command2 == "search":
            save_files_in_search_results_folder()
    elif command == 'update':
        update_imported_data()
    else:
        raise ValueError(f"command {command} not recognized")


if __name__ == "__main__":
    main()
