import polars as pl
import logging
import os

from config import SCHEMAPLAN_PATH, PROJECTFILE_PATH
from scripts.data_cleaner import deduplicate_dataframe, bitfix, dateloadedfix, create_postgis_geometry, numericfix, integerfix, add_or_update_project_key
from scripts.utils import schema_to_dictionary, generate_unique_constraint_standalone
from scripts.data_validator import dataframe_validator
from scripts.db_connector import insert_project, subset_and_save, populate_datevisited

logger = logging.getLogger(__name__)

def process_csv(file_name: str, project_key: str = None):
    logger.info(f"Starting process_csv function for file: {file_name}")
    # Load the CSV into a DataFrame with schemaplan fields
    filename = os.path.basename(file_name)
    table_name = os.path.splitext(filename)[0]
    logger.info(f"Extracted table name: {table_name}")

    try:
        # Validate with schemaplan
        logger.info(f"Loading CSV from data/{table_name}.csv")
        csv_df = pl.read_csv(f"data/{table_name}.csv", null_values=["NA", "N/A", "null"], infer_schema_length=10000000)
        csv_df = dataframe_validator(csv_df, table_name)
        logger.info(f"DataFrame validated for table: {table_name}")

        if csv_df is not None:
            # Cleaning functions: populate fields, clean fields, etc.
            logger.info(f'Working on: "{table_name}"...')

            # Add project_key if provided, otherwise check if present in the DataFrame
            if project_key is not None:
                csv_df = add_or_update_project_key(csv_df, project_key)
                logger.info(f"Project key '{project_key}' added/updated in DataFrame.")
            else:
                logger.info("Project key not provided. Checking for existing 'ProjectKey' in DataFrame...")
                if "ProjectKey" in csv_df.columns and csv_df[0].select(pl.col("ProjectKey").unique())[0, 0] is not None:
                    existing_project_key = csv_df[0].select(pl.col("ProjectKey").unique())[0, 0]
                    logger.info(f"Using 'ProjectKey' = {existing_project_key} found within CSV.")
                else:
                    logger.info("'ProjectKey' not found, proceeding with null 'ProjectKey' column.")

            # Additional data processing
            logger.info(f"Creating PostGIS geometry for table: {table_name}")
            csv_df = create_postgis_geometry(csv_df)

            logger.info(f"Fixing 'DateLoadedInDb' for table: {table_name}")
            csv_df = dateloadedfix(csv_df)

            logger.info(f"Deduplicating DataFrame for table: {table_name}")
            csv_df = deduplicate_dataframe(csv_df, generate_unique_constraint_standalone(table_name))

            # Apply schema corrections
            scheme = schema_to_dictionary(table_name)
            logger.info(f"Applying numeric fix for table: {table_name}")
            csv_df = numericfix(csv_df, scheme)

            logger.info(f"Applying integer fix for table: {table_name}")
            csv_df = integerfix(csv_df, scheme)

            logger.info(f"Applying bit fix for table: {table_name}")
            csv_df = bitfix(csv_df, scheme)

            # Populate 'DateVisited' if necessary
            if "dataHeader" not in table_name:
                logger.info(f"Not dataHeader! Checking for 'DateVisited' on: {table_name}")
                if "DateVisited" not in csv_df.columns:
                    logger.info(f"Adding 'DateVisited' column to table: {table_name}")
                    csv_df = csv_df.with_columns(pl.lit(None).alias("DateVisited"))
                if csv_df["DateVisited"].unique().to_list() == [None]:
                    logger.info(f"Populating 'DateVisited' for table: {table_name} (found None)")
                    csv_df = populate_datevisited(csv_df, table_name)

            logger.info(f"Finished processing CSV for table: {table_name}")

            schemaplankeys = schema_to_dictionary(table_name).keys()
            schemaplan_less = [col for col in csv_df.columns if col not in schemaplankeys]
            schemaplan_more = [col for col in schemaplankeys if col not in csv_df.columns]
            if len(schemaplan_more)>0 or len(schemaplan_less)>0:
                print(f"there's a mismatch between fields in schemaplan and fields in CSV..")
                print(f"selecting from schemaplan..")
                csv_df = csv_df.select(schemaplankeys)
            

            # Return the processed DataFrame and table name
            return {
                'table_name': table_name,
                'dataframe': csv_df
            }
        else:
            logger.warning(f"Validation failed for DataFrame of table: {table_name}")
            return None

    except Exception as e:
        logger.error(f"Error processing CSV for table '{table_name}': {e}")
        return None


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
