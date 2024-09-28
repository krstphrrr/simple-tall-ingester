from scripts.data_loader import process_csv, projectkey_extract, load_projecttable
from scripts.db_connector import insert_dataframe_to_db
# from scripts.utils import generate_unique_constraint_query
from config import DATA_DIR, DATABASE_CONFIG, SCHEMAPLAN_PATH, DBSCHEMA
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
        project_key = None
        # Check if 'debug' is in the argument
        if 'debug' in arg.split():
            logging.getLogger().setLevel(logging.DEBUG)  # Set root logger to DEBUG level
            logger.debug('Debug mode enabled.')

        # Ask for confirmation on TABLE_SCHEMA
        table_schema_confirm = input(f'Ingesting to TABLE_SCHEMA: {DBSCHEMA}. Continue? (y/n): ').strip().lower()
        if table_schema_confirm != 'y':
            logger.info("Aborting ingestion. Please update the TABLE_SCHEMA configuration.")
            return  # Exit the function if the user does not confirm

        # Ask for confirmation on DATABASE_CONFIG.host
        db_host_confirm = input(f'Ingesting to database: {DATABASE_CONFIG['host']}. Continue? (y/n): ').strip().lower()
        if db_host_confirm != 'y':
            logger.info("Aborting ingestion. Please update the DATABASE_CONFIG.host configuration.")
            return  # Exit the function if the user does not confirm

        # Ask for confirmation on SCHEMAPLAN_PATH
        schema_plan_confirm = input(f'Ingesting using SCHEMAPLAN: "{os.path.basename(SCHEMAPLAN_PATH)}". Continue? (y/n): ').strip().lower()
        if schema_plan_confirm != 'y':
            logger.info("Aborting ingestion. Please update the SCHEMAPLAN_PATH configuration.")
            return  # Exit the function if the user does not confirm

        data_dir = DATA_DIR

        # Initialize a list to hold CSV files
        csv_files = [file_name for file_name in os.listdir(data_dir) if file_name.endswith(".csv")]
        project_file = [file_name for file_name in os.listdir(data_dir) if file_name.endswith(".xlsx") and 'project' in file_name]

        if len(project_file)>0:
            logger.info("main:: project xlsx found, extracting projectkey")
            projectpath = os.path.join(DATA_DIR,project_file[0])
            load_projecttable(projectpath, 'tblProject')
            project_key = projectkey_extract()

        # Check if "dataHeader.csv" exists and process it first
        if "dataHeader.csv" in csv_files:
            file_path = os.path.join(data_dir, "dataHeader.csv")
            result = process_csv(file_path, project_key)
            df = result['dataframe']
            table_name = result['table_name']
            insert_dataframe_to_db(df, table_name)
            logger.info(f"main:: Ingested file: dataHeader.csv into table \"{table_name}\" ")
            csv_files.remove("dataHeader.csv")  # Remove it from the list to avoid reprocessing

        # Process remaining CSV files
        for file_name in csv_files:
            file_path = os.path.join(data_dir, file_name)
            result = process_csv(file_path, project_key)
            df = result['dataframe']
            table_name = result['table_name']
            insert_dataframe_to_db(df, table_name)

    # def do_generate(self, arg):
    #     """
    #     Command to generate a unique constraint for a given table.
    #     Usage: generate unique <tablename>
    #     """
    #     args = arg.split()

    #     if len(args) == 2 and args[0] == 'unique':
    #         table_name = args[1]
    #         try:
    #             query = generate_unique_constraint_query(table_name)
    #             print(f"Generated unique constraint query for table {table_name}:\n{query}")

    #         except Exception as e:
    #             print(f"Error generating unique constraint: {e}")
    #     else:
    #         print("Usage: generate unique <tablename>")



    def do_exit(self, arg):
        'Exit the CLI'
        logging.info('Exiting the CLI.')
        return True

if __name__ == '__main__':
    TallIngester().cmdloop()
