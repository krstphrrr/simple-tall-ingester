import polars as pl
import logging
from config import SCHEMAPLAN_PATH
from scripts.utils import map_pg_type_to_polars

logger = logging.getLogger(__name__)

def dataframe_validator(df: pl.DataFrame, tablename: str) -> pl.DataFrame:
    """
    validate the loaded dataframe with schemaplan by selecting columns directly from the
    """
    # load entire schemaplan
    schemaplan = pl.read_csv(SCHEMAPLAN_PATH, encoding='ISO-8859-1', schema_overrides={"Description": pl.Utf8})
    # list of column names
    fieldnames = [i for i in schemaplan.filter(pl.col("Table")==tablename)['Field']]
    # list of corresponding types (converted from postgres types to polars types)
    fieldtypes = [map_pg_type_to_polars(i) for i in schemaplan.filter(pl.col("Table")==tablename)['DataType']]

    # create empty dataframe with the proper columns and dtypes 
    schemaplan_fields = pl.DataFrame(
        {fieldnames: pl.Series(fieldnames, dtype=dtype) for fieldnames, dtype in zip(fieldnames, fieldtypes)}
    )

    # select which common columns between both df's so there are no surprises from the csv df
    common_columns = [col for col in df.columns if col in schemaplan_fields.columns]
    df = df.select(common_columns)

    # add any column missing from the df PRESENT IN THE SCHEMAPLAN back in, and if it doesn't exist make it null
    for col in schemaplan_fields.columns:
        if col not in df.columns:
            logger.info(f"dataframe_validator added field:\"{col}\" to \"{tablename}\"")
            df = df.with_columns(pl.lit(None).alias(col))


    # select entire dataframe including the potentially null colums that were just added
    df = df.select(schemaplan_fields.columns)
    return df

