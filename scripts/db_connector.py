import psycopg2
from sqlalchemy import create_engine
from config import DATABASE_CONFIG, DBSCHEMA, SCHEMAPLAN_PATH, NOPRIMARYKEYPATH
from scripts.utils import generate_unique_constraint_query
import polars as pl
import logging
import os

import psycopg2
from psycopg2 import sql

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
    

def subset_and_save(tabledf: pl.DataFrame, table_name: str) -> pl.DataFrame:
    logger.info(f'Matching "PrimaryKeys" with header.. a subset of "{table_name}" will be produced in the ./noprimarykey dir if mismatches are found.')
    # if dbkey is required, add extraction here here
    try:
        connection = psycopg2.connect(**DATABASE_CONFIG)
        # Query the "dataHeader" table and load it into a Polars DataFrame
        query = f'SELECT "PrimaryKey" FROM {DBSCHEMA}."dataHeader";'
        with connection.cursor() as cursor:
            cursor.execute(query)
            # Fetch all results into a DataFrame
            data = cursor.fetchall()
            dataHeader_df = pl.DataFrame(data, orient="row", schema=["PrimaryKey"])
            cursor.close()
    
        # Extract the unique PrimaryKey values from the dataHeader DataFrame
        primary_keys = dataHeader_df.select(pl.col("PrimaryKey"))

        # Filter tblRHEM_df where PrimaryKey exists in the primary_keys list
        matching_df = tabledf.join(primary_keys, on="PrimaryKey", how="inner")

        # Filter tblRHEM_df where PrimaryKey does not exist in the primary_keys list
        non_matching_df = tabledf.join(primary_keys, on="PrimaryKey", how="anti")

        # Save the non-matching part to a CSV file
        non_matching_csv_file = os.path.join(NOPRIMARYKEYPATH,f"no_primarykeys_{table_name}.csv")
        non_matching_df.write_csv(non_matching_csv_file)

        # Return the matching subset for ingestion
        return matching_df
    finally:
        #  Ensure the connection is closed
        connection.close()

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
        #
        # # Create an index on the 'rid' column
        cursor.execute(f'CREATE INDEX IF NOT EXISTS {table_name}_rid_idx ON {DBSCHEMA}."{table_name}" (rid);')
        conn.commit()

        unique_constraint_query = generate_unique_constraint_query(table_name)
        cursor.execute(unique_constraint_query)
        cursor.commit()

        cursor.close()


    except Exception as e:
        if conn:
            conn.rollback()

        logger.info(f"Error creating table {table_name}: {e}")
    finally:
        if conn:
            conn.close()


def insert_dataframe_to_db(df: pl.DataFrame, table_name: str, geometry_column: str = None, srid: int = 4326):
    create_table_if_not_exists(table_name)  # Ensure table exists before inserting data

    conn = None
    try:
        # Convert DataFrame to list of tuples for insertion
        records = df.to_dicts()

        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()

        cols = ", ".join([f'"{col}"' for col in df.columns])
        # placeholders = ", ".join(["%s"] * len(df.columns))

        # query = f"""
        # INSERT INTO {DBSCHEMA}."{table_name}" ({cols})
        # VALUES ({placeholders})
        # """

        # Insert records into the database
        for record in records:
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

        conn.commit()
        cursor.close()
        logger.info(f"Inserted into table \"{table_name}\" ")

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

        logger.info(f"Error creating table {tablename}: {e}")
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
