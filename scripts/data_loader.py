import polars as pl
import logging
import os
from config import SCHEMAPLAN_PATH
from scripts.data_cleaner import deduplicate_dataframe, bitfix, dateloadedfix, create_postgis_geometry, numericfix, integerfix
from scripts.utils import schema_to_dictionary
from scripts.data_validator import dataframe_validator

logger = logging.getLogger(__name__)

def process_csv(file_name: str):
    # Load the CSV into a DataFrame with schemaplan fields
    filename = os.path.basename(file_name)
    table_name = os.path.splitext(filename)[0]
    
    # validate with schemaplan
    csv_df = pl.read_csv(f"data/{table_name}.csv")
    csv_df = dataframe_validator(csv_df, table_name)
 
    if csv_df is not None:
        # Cleaning functions: populate fields, clean fields etc.
        # TODOdf = populate datevisited
        logger.info(f'Working on: "{table_name}" ')

        csv_df = create_postgis_geometry(csv_df)
        csv_df = dateloadedfix(csv_df)
        csv_df = deduplicate_dataframe(csv_df)

        scheme = schema_to_dictionary(table_name)
        csv_df = numericfix(csv_df,scheme)
        csv_df = integerfix(csv_df,scheme)
        csv_df = bitfix(csv_df, scheme)

        # Insert the DataFrame into the target table (includes table creation)
        return {
            'table_name': table_name,
            'dataframe': csv_df
        }
        # insert_dataframe_to_db(df, target_table)
        # logger.info(f"Processed {file_name} into table {target_table} with source {source}.")
