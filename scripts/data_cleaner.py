from config import TODAYS_DATE
import polars as pl
# from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def add_or_update_project_key(csv_df: pl.DataFrame, project_key: str) -> pl.DataFrame:

    # Check if 'ProjectKey' column exists in the DataFrame
    if 'ProjectKey' in csv_df.columns:
        # Update existing 'ProjectKey' column with the given project_key value
        csv_df = csv_df.with_columns(pl.lit(project_key).alias('ProjectKey'))
    else:
        # Add a new 'ProjectKey' column with the given project_key value
        csv_df = csv_df.with_columns(pl.lit(project_key).alias('ProjectKey'))

    return csv_df

def deduplicate_dataframe(df: pl.DataFrame, subset:list[str]) -> pl.DataFrame:
    return df.unique(subset=subset)

def dateloadedfix(df: pl.DataFrame) -> pl.DataFrame:
    # current_date = datetime.now().strftime('%Y-%m-%d')
    current_date = TODAYS_DATE

    df = df.with_columns(pl.lit(None).alias("DateLoadedInDb"))
    df = df.with_columns(
        pl.when(pl.col('DateLoadedInDb').is_null())
        .then(pl.lit(current_date))
        # .otherwise(pl.col('DateLoadedInDb'))
        # .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False)  # Correct format argument
        .cast(pl.Date)
        .alias('DateLoadedInDb')
    )
    return df

def create_postgis_geometry(df: pl.DataFrame) -> pl.DataFrame:
    if "wkb_geometry" in df.columns:
        df = df.with_columns(pl.lit(None).alias('wkb_geometry'))
        df = df.with_columns(
            pl.when(pl.col('wkb_geometry').is_null())
            .then(pl.format("POINT({} {})", pl.col('Longitude_NAD83'), pl.col('Latitude_NAD83')))
            .otherwise(pl.col('wkb_geometry'))
            .alias('wkb_geometry')
        )
    else:
        pass

    return df

def numericfix(df: pl.DataFrame, colscheme: dict) -> pl.DataFrame:
    for i in df.columns:
        # if column SHOULD be numeric, but is string
        if colscheme[i].lower() == 'numeric' and df[i].dtype==pl.Utf8:
            logging.debug(f'data_cleaner.numericfix is casting col "{i}" from str to numeric')
            df = df.with_columns(
                pl.when(pl.col(i) == "NA")
                .then(pl.lit(None))
                .otherwise(pl.col(i))
                .alias(i)
            )
            # casting to float
            df = df.with_columns(
                pl.col(i).cast(pl.Float64).alias(i)
            )
        # if column SHOULD be numeric, but is integer
        elif colscheme[i].lower() == 'numeric' and df[i].dtype==pl.Int64:
            logging.debug(f'data_cleaner.numericfix is casting col "{i}" from int to numeric')
            df = df.with_columns(
                pl.col(i).cast(pl.Float64).alias(i)
            )
    return df

def integerfix(df: pl.DataFrame, colscheme: dict) -> pl.DataFrame:
    for i in df.columns:
        # if column SHOULD be integer, but is string
        if colscheme[i].lower() == 'integer' and df[i].dtype==pl.Utf8:
            logging.debug(f'data_cleaner.integerfix is casting col "{i}" from str to integer')
            df = df.with_columns(
                pl.when(pl.col(i) == "NA")
                .then(pl.lit(None))
                .otherwise(pl.col(i))
                .alias(i)
            )
            # casting to int
            df = df.with_columns(
                pl.col(i).cast(pl.Int64).alias(i)
            )
        # if column SHOULD be integer, but is numeric
        elif colscheme[i].lower() == 'integer' and df[i].dtype==pl.Float64:
            logging.debug(f'data_cleaner.integerfix is casting col "{i}" from numeric to integer')
            df = df.with_columns(
                pl.col(i).cast(pl.Int64).alias(i)
            )
    return df

def bitfix(df: pl.DataFrame, colscheme: dict) -> pl.DataFrame:
    for i in df.columns:
        # Handle string types that need conversion to bits
        if colscheme[i].lower() == 'bit' and df.select(pl.col(i)).dtypes[0] == pl.Utf8:
            logging.debug(f'data_cleaner.bitfix is processing col "{i}" to bit (found string)')
            df = df.with_columns(pl.when(pl.col(i) == '').then(None))
            if df[i].is_in(["TRUE", "FALSE"]).any():
                df = df.with_columns(
                    pl.when(pl.col(i) == "TRUE").then(1)
                    .when(pl.col(i) == "FALSE").then(0)
                    .otherwise(pl.col(i))
                    .alias(i)
                )

            elif df[i].is_in(["Y", "N"]).any():
                df = df.with_columns(
                    pl.when(pl.col(i) == "Y").then(1)
                    .when(pl.col(i) == "N").then(0)
                    .otherwise(pl.col(i))
                    .alias(i)
                )

            elif df[i].is_in(["L", "D", ""]).any():
                df = df.with_columns(
                    pl.when(pl.col(i) == "D").then(1)
                    .when(pl.col(i) == "L").then(0)
                    .when(pl.col(i) == "").then(None)
                    .otherwise(pl.col(i))
                    .alias(i)
                )


            elif df[i].is_in(["0", "1"]).any():
                df = df.with_columns(
                    pl.when(pl.col(i).is_null()).then(None)
                    .when(pl.col(i) == "1").then(1)
                    .when(pl.col(i) == "0").then(0)
                    .otherwise(pl.col(i))
                    .alias(i)
                )

            # Convert column to string
            df = df.with_columns(pl.col(i).cast(pl.Utf8).alias(i))

        # Handle case where the column is a boolean type
        elif colscheme[i].lower() == 'bit' and df.select(pl.col(i)).dtypes[0] == pl.Boolean:
            logging.debug(f'data_cleaner.bitfix is processing col "{i}" to bit (found boolean)')
            df = df.with_columns(pl.col(i).cast(pl.Utf8).alias(i))
            df = df.with_columns(
                pl.when(pl.col(i) == "TRUE".lower()).then(1)
                .when(pl.col(i) == "FALSE".lower()).then(0)
                .otherwise(pl.col(i))
                .alias(i)
            )

        elif colscheme[i].lower() == 'bit' and df.select(pl.col(i)).dtypes[0] == pl.Int64:
            logging.debug(f'data_cleaner.bitfix is processing col "{i}" to bit (found integer)')
            df = df.with_columns(
                pl.when(pl.col(i).is_null()).then(pl.lit(None))
                .when(pl.col(i) == pl.lit(1)).then(pl.lit('1'))
                .when(pl.col(i) == pl.lit(0)).then(pl.lit('0'))
                .otherwise(pl.lit(None))
                .alias(i)
            )
    return df
