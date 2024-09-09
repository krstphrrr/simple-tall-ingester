from scripts.data_loader import process_csv
from scripts.db_connector import insert_dataframe_to_db
from config import DATA_DIR
import logging
import cmd
import os

logger = logging.getLogger(__name__)


class TallIngester(cmd.Cmd):
    intro = 'Welcome to the tall table ingester. Type help or ? to list commands.\n'
    prompt = '(tall-table-ingester) '

    def __init__(self):
        super().__init__()



    def do_ingest(self, arg):
        data_dir = DATA_DIR

        # Initialize a list to hold CSV files
        csv_files = [file_name for file_name in os.listdir(data_dir) if file_name.endswith(".csv")]

        # Check if "dataHeader.csv" exists and process it first
        if "dataHeader.csv" in csv_files:
            file_path = os.path.join(data_dir, "dataHeader.csv")
            result = process_csv(file_path)
            df = result['dataframe']
            table_name = result['table_name']
            insert_dataframe_to_db(df, table_name)
            logger.info(f"Ingested file: dataHeader.csv into table \"{table_name}\" ")
            csv_files.remove("dataHeader.csv")  # Remove it from the list to avoid reprocessing

        # Process remaining CSV files
        for file_name in csv_files:
            file_path = os.path.join(data_dir, file_name)
            result = process_csv(file_path)
            df = result['dataframe']
            table_name = result['table_name']
            insert_dataframe_to_db(df, table_name)
            


    def do_exit(self, arg):
        'Exit the CLI'
        logging.info('Exiting the CLI.')
        return True

if __name__ == '__main__':
    TallIngester().cmdloop()
