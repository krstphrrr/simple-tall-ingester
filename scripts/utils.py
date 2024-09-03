from config import SCHEMAPLAN_PATH
from scripts.data_cleaner import deduplicate_dataframe

import polars as pl
import logging
import os


logger = logging.getLogger(__name__)
def process_csv(file_name: str):
    # MOVE LOADING TO LOADER

    # Load the CSV into a DataFrame with schemaplan fields
    filename = os.path.basename(file_name)
    table_name = os.path.splitext(filename)[0]
    schemaplan = pl.read_csv(SCHEMAPLAN_PATH, encoding='ISO-8859-1', schema_overrides={"Description": pl.Utf8})
    fieldnames = [i for i in schemaplan.filter(pl.col("Table")==table_name)['Field']]
    fieldtypes = [map_pg_type_to_polars(i) for i in schemaplan.filter(pl.col("Table")==table_name)['DataType']]

    schemaplan_fields = pl.DataFrame(
        {fieldnames: pl.Series(fieldnames, dtype=dtype) for fieldnames, dtype in zip(fieldnames, fieldtypes)}
    )
    csv_df = pl.read_csv(f"data/{table_name}.csv")

    common_columns = [col for col in csv_df.columns if col in schemaplan_fields.columns]

    csv_df = csv_df.select(common_columns)

    for col in schemaplan_fields.columns:
        if col not in csv_df.columns:
            csv_df = csv_df.with_columns(pl.lit(None).alias(col))

    csv_df = csv_df.select(schemaplan_fields.columns)


    if csv_df is not None:


        # Cleaning functions: populate fields, clean fields etc.
        # TODOdf = populate datevisited
        # TODOdf = populate dateloadedindb
        csv_df = deduplicate_dataframe(csv_df)

        # Insert the DataFrame into the target table (includes table creation)
        print(csv_df)
        # insert_dataframe_to_db(df, target_table)
        # logger.info(f"Processed {file_name} into table {target_table} with source {source}.")

def map_pg_type_to_polars(pg_type: str) -> pl.DataType:
    # Mapping PostgreSQL types to Polars types
    pg_to_polars_map = {
        "integer": pl.Int32,
        "int": pl.Int32,
        "smallint": pl.Int16,
        "bigint": pl.Int64,
        "serial": pl.Int32,
        "bigserial": pl.Int64,
        "real": pl.Float32,
        "double precision": pl.Float64,
        "numeric": pl.Float64,
        "decimal": pl.Float64,
        "boolean": pl.Boolean,
        "bit": pl.Boolean,
        "text": pl.Utf8,
        "varchar": pl.Utf8,
        "char": pl.Utf8,
        "character varying": pl.Utf8,
        "character": pl.Utf8,
        "date": pl.Date,
        "timestamp": pl.Datetime,
        "timestamp without time zone": pl.Datetime,
        "timestamp with time zone": pl.Datetime,
        "time": pl.Time,
        "time without time zone": pl.Time,
        "time with time zone": pl.Time,
        "json": pl.Object,  # or pl.Utf8 if you want to keep it as string
        "jsonb": pl.Object,  # or pl.Utf8
        "uuid": pl.Utf8,
        "bytea": pl.Binary,
    }

    # Convert to lower case for case-insensitive matching
    pg_type_lower = pg_type.lower()

    # Return the corresponding polars type or None if not found
    return pg_to_polars_map.get(pg_type_lower, None)
