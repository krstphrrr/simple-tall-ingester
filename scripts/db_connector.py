import psycopg2
from sqlalchemy import create_engine
from config import DATABASE_CONFIG, DBSCHEMA, SCHEMAPLAN_PATH, NOPRIMARYKEYPATH
from scripts.utils import generate_unique_constraint_query
import polars as pl
import logging
import os

import psycopg2
from psycopg2 import sql
from tqdm import tqdm
import math

logger = logging.getLogger(__name__)

def map_dtype_to_sql(dtype: pl.DataType) -> str:
    if dtype == pl.Int64 or dtype == pl.Int32:
        return "INTEGER"
    elif dtype == pl.Float64 or dtype == pl.Float32:
        return "FLOAT"
    elif dtype == pl.Date or dtype == pl.Datetime:
        return "DATE"
    else:
        return "TEXT"



def create_table_if_not_exists(table_name: str):
    conn = None
    try:

        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()

        schemaplan = pl.read_csv(SCHEMAPLAN_PATH, encoding='ISO-8859-1', schema_overrides={"Description": pl.Utf8})

        fieldnames = [i for i in schemaplan.filter(pl.col("Table")==table_name)['Field']]
        fieldtypes = [i for i in schemaplan.filter(pl.col("Table")==table_name)['DataType']]
        # columns = ["rid SERIAL PRIMARY KEY"] + [i for i in schemaplan.filter(pl.col("Table")==table_name)['Field']]
        columns = ["rid SERIAL PRIMARY KEY"] + [f'"{col}" {sqltype}' for col, sqltype in zip(fieldnames, fieldtypes)]


        # Add constraints based on the table name
        if table_name.lower() == "dataheader":
            # If the table is "dataHeader", add a UNIQUE constraint to the "PrimaryKey" column
            columns.append(f'UNIQUE ("PrimaryKey")')
        else:
            # If the table is not "dataHeader", add a FOREIGN KEY constraint
            columns.append(f'FOREIGN KEY ("PrimaryKey") REFERENCES {DBSCHEMA}."dataHeader"("PrimaryKey")')
        columns_sql = ', '.join(columns)
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {DBSCHEMA}."{table_name}" (
            {columns_sql}
        );
        """

        cursor.execute(create_table_query)
        conn.commit()


        cursor.close()


    except Exception as e:
        if conn:
            conn.rollback()

        logger.info(f"Error creating table {table_name}: {e}")
    finally:
        if conn:
            conn.close()


def create_index_if_not_exist(table_name):
    conn = None
    try:

        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()
        cursor.execute(f'CREATE INDEX IF NOT EXISTS {table_name}_rid_idx ON {DBSCHEMA}."{table_name}" (rid);')
        conn.commit()


    except Exception as e:
        if conn:
            conn.rollback()

        logger.info(f"db_connector::create_index:: Error creating index on {table_name}: {e}")
    finally:
        if conn:
            conn.close()

def create_unique_constraint_if_not_exist(table_name):
    conn = None
    try:

        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()
        unique_constraint_query = generate_unique_constraint_query(table_name)
        cursor.execute(unique_constraint_query)
        conn.commit()

    except Exception as e:
        if conn:
            conn.rollback()

        logger.info(f"db_connector::create_unique:: Error creating unique constraint on {table_name}: {e}")
    finally:
        if conn:
            conn.close()

def insert_dataframe_to_db(df: pl.DataFrame, table_name: str, geometry_column: str = None, srid: int = 4326, chunk_size: int = 10000):
    create_table_if_not_exists(table_name)  # Ensure table exists before inserting data
    create_index_if_not_exist(table_name)
    create_unique_constraint_if_not_exist(table_name)

    conn = None
    try:
        # Convert DataFrame to list of tuples for insertion
        records = df.to_dicts()
        total_records = len(records)
        total_chunks = math.ceil(total_records / chunk_size)

        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()

        cols = ", ".join([f'"{col}"' for col in df.columns])

        for chunk_idx in tqdm(range(total_chunks), desc="Inserting chunks", unit="chunk"):
            start_idx = chunk_idx * chunk_size
            end_idx = min(start_idx + chunk_size, total_records)
            chunk = records[start_idx:end_idx]

            for record in chunk:
                values = []
                for col in df.columns:
                    if col == geometry_column:
                        # Use ST_GeomFromText to convert WKT to geometry
                        values.append(f"ST_SetSRID(ST_GeomFromText(%s), {srid})")
                    else:
                        values.append("%s")

                # Construct the query with dynamic placeholders
                insert_query = f"""
                    INSERT INTO {DBSCHEMA}."{table_name}" ({cols})
                    VALUES ({', '.join(values)})
                    """

                # Prepare the record values, using geometry conversion when necessary
                record_values = tuple(record[col] if col != geometry_column else record[col] for col in df.columns)

                # Execute the query
                cursor.execute(insert_query, record_values)

            # Commit after each chunk is processed
            conn.commit()

        cursor.close()
        logger.info(f"All chunks inserted into table \"{table_name}\".")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error inserting DataFrame into DB: {e}")
    finally:
        if conn:
            conn.close()


def create_projecttable(columns: list[str], tablename: str):
    conn = psycopg2.connect(**DATABASE_CONFIG)

    # Dynamically create the table if it doesn't exist
    create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {db_schema}.{table} (
            {fields} , unique("project_key")
            )
        """).format(
            db_schema=sql.Identifier(DBSCHEMA),
            table=sql.Identifier(tablename),
            fields=sql.SQL(', ').join(sql.Identifier(col) + sql.SQL(' TEXT') for col in columns)  # Assuming all fields are TEXT type; adjust as needed
        )

    try:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()

    except Exception as e:
        if conn:
            conn.rollback()

        logger.info(f"db_connector::create_project:: Error creating table {tablename}: {e}")
    finally:
        if conn:
            conn.close()

def insert_project(values: list[str], columns: list[str], tablename: str):
    # create if not exist
    create_projecttable(columns, tablename)

    conn = psycopg2.connect(**DATABASE_CONFIG)
    # Insert data into the table
    insert_query = sql.SQL("""
            INSERT INTO {db_schema}.{table} ({columns})
            VALUES ({placeholders})
        """).format(
            db_schema = sql.Identifier(DBSCHEMA),
            table=sql.Identifier(tablename),
            columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
            placeholders=sql.SQL(', ').join(sql.Placeholder() * len(values))
        )

    try:
        cursor = conn.cursor()
        cursor.execute(insert_query, values)
        conn.commit()
        cursor.close()

    except Exception as e:
        if conn:
            conn.rollback()

        logger.info(f"Error inserting into table {tablename}: {e}")
    finally:
        if conn:
            conn.close()

def subset_and_save(table_df: pl.DataFrame, table_name: str) -> pl.DataFrame:
    logger.info(f'db_connector: Matching "PrimaryKeys" with header.. a subset of "{table_name}" will be produced in the ./noprimarykey dir if mismatches are found.')
    # if dbkey is required, add extraction here here
    # dbkey check and add for csv's with not primarykeys
    dbkey = None
    if "DBKey" in table_df.columns:
        dbkey = table_df[0].select(pl.col("DBKey").unique())[0,0]
    else:
        dbkey="nodbkey"


    try:
        connection = psycopg2.connect(**DATABASE_CONFIG)
        # Query the "dataHeader" table and load it into a Polars DataFrame
        query = f'SELECT "PrimaryKey" FROM {DBSCHEMA}."dataHeader";'
        with connection.cursor() as cursor:
            cursor.execute(query)
            # Fetch all results into a DataFrame
            data = cursor.fetchall()
            dataHeader_df = pl.DataFrame(data, orient="row", schema=["PrimaryKey"])

        # Extract the unique PrimaryKey values from the dataHeader DataFrame
        primary_keys = dataHeader_df.select(pl.col("PrimaryKey"))

        # Filter tblRHEM_df where PrimaryKey exists in the primary_keys list
        matching_df = table_df.join(primary_keys, on="PrimaryKey", how="inner")

        # Filter tblRHEM_df where PrimaryKey does not exist in the primary_keys list
        non_matching_df = table_df.join(primary_keys, on="PrimaryKey", how="anti")

        # Save the non-matching part to a CSV file
        non_matching_csv_file = os.path.join(NOPRIMARYKEYPATH,f"no_primarykeys_{dbkey}_{table_name}.csv")
        if non_matching_df.shape[0] != 0:
            non_matching_df.write_csv(non_matching_csv_file)

        # Return the matching subset for ingestion
        return matching_df
    except Exception as e:
        logger.info(f"db_connector::subset_pk:: error: {e}")
    finally:
        connection.close()

def populate_datevisited(table_df: pl.DataFrame, table_name: str) -> pl.DataFrame:
    logger.info(f'populate_datevisited: Matching "DateVisited" on {table_name} to dataHeader...')

    try:
        logger.info("populate_datevisited: Establishing database connection.")
        connection = psycopg2.connect(**DATABASE_CONFIG)
        logger.info("populate_datevisited: Database connection established.")
        primary_keys = table_df["PrimaryKey"].to_list()
        logger.info(f"populate_datevisited: Retrieved {len(primary_keys)} primary keys from table {table_name}.")

        # Use a single query to fetch all relevant data, avoiding batch processing
        placeholders = ', '.join(['%s'] * len(primary_keys))  # Prepare placeholders for the IN clause
        query = f'SELECT "PrimaryKey", "DateVisited" FROM {DBSCHEMA}."dataHeader" WHERE "PrimaryKey" IN ({placeholders});'
        logger.debug(f"populate_datevisited: Executing query: {query}")

        with connection.cursor() as cursor:
            cursor.execute(query, primary_keys)
            data = cursor.fetchall()
            logger.info(f"populate_datevisited: Fetched {len(data)} records from dataHeader.")

            if data:  # Only process if data was returned
                dataHeader_combined_df = pl.DataFrame(data, schema=["PrimaryKey", "DateVisited"])
            else:
                logger.warning("populate_datevisited: No matching primary keys found in dataHeader.")
                return table_df  # No matches found, return the original DataFrame

        # Perform the join with the combined dataHeader DataFrame
        logger.info("populate_datevisited: Performing join with the combined dataHeader DataFrame.")
        merged_df = table_df.join(dataHeader_combined_df, on="PrimaryKey", how="left")

        # Handle updating or renaming the "DateVisited" column
        if "DateVisited" in table_df.columns:
            logger.info("populate_datevisited: Updating existing 'DateVisited' column with non-null values from dataHeader.")
            # Replace only where the joined DateVisited from dataHeader is non-null
            merged_df = merged_df.with_columns(
                pl.when(merged_df["DateVisited_right"].is_not_null())
                .then(merged_df["DateVisited_right"])
                .otherwise(merged_df["DateVisited"])  # Retain original DateVisited where no match
                .alias("DateVisited")
            ).drop("DateVisited_right")
        else:
            logger.info("populate_datevisited: Adding 'DateVisited' column from dataHeader.")
            # If "DateVisited" doesn't exist, just rename the merged "DateVisited_right" column
            merged_df = merged_df.rename({"DateVisited_right": "DateVisited"})

        # Return the updated DataFrame with the DateVisited column populated
        logger.info("populate_datevisited: Successfully populated 'DateVisited' column.")
        return merged_df

    except Exception as e:
        logger.error(f"populate_datevisited: Error: {e}")
        return table_df  # Return original DataFrame on error

    finally:
        connection.close()
        logger.info("populate_datevisited: Database connection closed.")
