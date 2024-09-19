import polars as pl
import logging
import os


from config import SCHEMAPLAN_PATH, PROJECTFILE_PATH
from scripts.data_cleaner import deduplicate_dataframe, bitfix, dateloadedfix, create_postgis_geometry, numericfix, integerfix, add_or_update_project_key
from scripts.utils import schema_to_dictionary
from scripts.data_validator import dataframe_validator
from scripts.db_connector import insert_project, subset_and_save, populate_datevisited

logger = logging.getLogger(__name__)

def process_csv(file_name: str, project_key: str = None):
    # Load the CSV into a DataFrame with schemaplan fields
    filename = os.path.basename(file_name)
    table_name = os.path.splitext(filename)[0]

    # validate with schemaplan
    csv_df = pl.read_csv(f"data/{table_name}.csv", null_values=["NA", "N/A", "null"], infer_schema_length=100000)
    csv_df = dataframe_validator(csv_df, table_name)

    if csv_df is not None:
        # Cleaning functions: populate fields, clean fields etc.
        # TODOdf = populate datevisited
        logger.info(f'Working on: "{table_name}"...')

        # add projectkey_populate (which in turn projectkey_extract)
        # creaate/insert projectkey
        if project_key is not None:
            csv_df = add_or_update_project_key(csv_df, project_key)
        else:
            logger.info("data_loader:: Project xlsx not found within data directory. Scanning for \"ProjectKey\" within dataframe..")
            if "ProjectKey" in csv_df.columns and csv_df[0].select(pl.col("ProjectKey").unique())[0,0] is not None:
                existing_project_key = csv_df[0].select(pl.col("ProjectKey").unique())[0,0]
                logger.info(f'data_loader:: Using "ProjectKey" = {existing_project_key} found within csv')
            else:
                logger.info("data_loader:: \"ProjectKey\" not found, proceeding with null \"ProjectKey\" column.")
        """
        - do we want to slowly check if the subset of primary keys we are about to ingest
        exist already in dataheader? if so:
            - we continue with no further checks 

        - do we want to *only* check for primarykey matches if the table about to be ingested
        did not come with an accompanying dataheader??? if so, add: 
            - if len([i for i in os.listdir(DATA_DIR) if 'dataHeader' in i])==0:
            - this will check if there is an accopanying dataheader.csv inside the datadir,
            only proceed if the dataheader is missing
            - the point is double check that the table we are ingesting maintains relational
            integrity with dataheader be it as the datapacket itself does not include a dataheader
        """
        if "dataHeader" not in table_name:
            # add ^^^ change here
            csv_df = subset_and_save(csv_df, table_name)


        csv_df = create_postgis_geometry(csv_df)
        csv_df = dateloadedfix(csv_df)
        csv_df = deduplicate_dataframe(csv_df)

        scheme = schema_to_dictionary(table_name)
        csv_df = numericfix(csv_df,scheme)
        csv_df = integerfix(csv_df,scheme)
        csv_df = bitfix(csv_df, scheme)

        if "DateVisited" in csv_df.columns and "dataHeader" not in table_name:
            if csv_df[0].select(pl.col("DateVisited").unique())[0,0] is None:
                logger.info(f"data_loader:: populating datevisited on {table_name} (found None)")
                csv_df = populate_datevisited(csv_df,table_name)

        # Insert the DataFrame into the target table (includes table creation)
        return {
            'table_name': table_name,
            'dataframe': csv_df
        }
        # insert_dataframe_to_db(df, target_table)
        # logger.info(f"Processed {file_name} into table {target_table} with source {source}.")


def load_projecttable(excel_path: str, table_name: str):
    # Read the Excel file into a Polars DataFrame
    df = pl.read_excel(excel_path, sheet_id=0)['Sheet1']
    logger.debug(f"DF COLUMNS: {df.columns}")

    # Extract column names and values from the DataFrame
    column_names = df['Var'].to_list()
    values = df['Value'].to_list()

    # Create table/insert data
    insert_project(values, column_names, table_name)

    logger.info(f"data_loader:: Data inserted successfully into {table_name}")

def projectkey_extract():
    # load project path
    basename = [i for i in os.listdir(PROJECTFILE_PATH) if 'project' in i][0]
    projectpath = os.path.join(PROJECTFILE_PATH, basename)

    df = pl.read_excel(projectpath, sheet_id=0)['Sheet1']

    # Extract column names and values from the DataFrame
    column_names = df['Var'].to_list()
    values = df['Value'].to_list()

    data_dict = {column_names[i]: [values[i]] for i in range(len(column_names))}
    new_df = pl.DataFrame(data_dict)
    return new_df['project_key'].to_list()[0]
