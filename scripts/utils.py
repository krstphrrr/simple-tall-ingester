from config import SCHEMAPLAN_PATH
from scripts.data_cleaner import deduplicate_dataframe, bitfix, dateloadedfix, create_postgis_geometry, numericfix, integerfix

import polars as pl
import logging
import os


logger = logging.getLogger(__name__)

def schema_chooser(tablename):
    fields = pl.read_csv(SCHEMAPLAN_PATH, encoding='ISO-8859-1', schema_overrides={"Description": pl.Utf8})
    return fields.filter(pl.col("Table")==tablename)

def schema_to_dictionary(tablename):
    schema = schema_chooser(tablename)
    return dict(zip(schema['Field'].to_list(), schema['DataType'].to_list()))


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
