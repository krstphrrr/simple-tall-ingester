import psycopg2
from config import DATABASE_CONFIG, DBSCHEMA, SCHEMAPLAN_PATH
import polars as pl
import logging


def map_dtype_to_sql(dtype: pl.DataType) -> str:
    if dtype == pl.Int64 or dtype == pl.Int32:
        return "INTEGER"
    elif dtype == pl.Float64 or dtype == pl.Float32:
        return "FLOAT"
    elif dtype == pl.Date or dtype == pl.Datetime:
        return "DATE"
    else:
        return "TEXT"

def create_table_if_not_exists(df: pl.DataFrame, table_name: str):
    conn = None
    try:

        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()

        schemaplan = pl.read_csv(SCHEMAPLAN_PATH, encoding='ISO-8859-1', schema_overrides={"Description": pl.Utf8})
        filtered_df = schemaplan.filter(pl.col('Table') == table_name)

        fieldnames = [i for i in schemaplan.filter(pl.col("Table")==table_name)['Field']]
        fieldtypes = [i for i in schemaplan.filter(pl.col("Table")==table_name)['DataType']]
        # columns = ["rid SERIAL PRIMARY KEY"] + [i for i in schemaplan.filter(pl.col("Table")==table_name)['Field']]
        columns = ["rid SERIAL PRIMARY KEY"] + [f'"{col}" {sqltype}' for col, sqltype in zip(fieldnames, fieldtypes)]

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

        cursor.close()


    except Exception as e:
        if conn:
            conn.rollback()

        logger.info(f"Error creating table {table_name}: {e}")
    finally:
        if conn:
            conn.close()


def insert_dataframe_to_db(df: pl.DataFrame, table_name: str):
    create_table_if_not_exists(df, table_name)  # Ensure table exists before inserting data

    conn = None
    try:
        # Convert DataFrame to list of tuples for insertion
        records = df.to_dicts()

        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()

        cols = ", ".join([f'"{col}"' for col in df.columns])
        placeholders = ", ".join(["%s"] * len(df.columns))

        query = f"""
        INSERT INTO {DBSCHEMA}."{table_name}" ({cols})
        VALUES ({placeholders})
        """

        for record in records:
            cursor.execute(query, tuple(record.values()))

        conn.commit()
        cursor.close()

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error inserting DataFrame into DB: {e}")
    finally:
        if conn:
            conn.close()
