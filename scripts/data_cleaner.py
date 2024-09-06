from config import TODAYS_DATE
import polars as pl

def add_date_loaded_column(df: pl.DataFrame) -> pl.DataFrame:
    try:
        df = df.with_columns(pl.lit(TODAYS_DATE).alias("DateLoadedInDb"))
        return df
    except Exception as e:
        print(f"Error adding DateLoadedInDb column: {e}")
        return None

def deduplicate_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    return df.unique()

def bitfix(df: pl.DataFrame, colscheme: dict) -> pl.DataFrame:
    for i in df.columns:

        if colscheme[i].lower() == 'bit' and df.select(pl.col(i)).dtypes[0]==pl.String:
            if df[i].is_in(["TRUE", "FALSE"]).any():

                df = df.with_column(
                    pl.when((pl.col(i).str.contains("0")) & (pl.col(i).is_str()))
                    .then(0)
                    .otherwise(pl.col(i))
                    .alias(i)
                )
                df = df.with_column(
                    pl.when((pl.col(i).str.contains("2")) & (pl.col(i).is_str()))
                    .then(1)
                    .otherwise(pl.col(i))
                    .alias(i)
                )

                df = df.with_column(
                    pl.when(pl.col(i) == "TRUE").then(1).when(pl.col(i) == "FALSE").then(0).otherwise(pl.col(i)).alias(i)
                )

            elif df[i].is_in(["Y", "N"]).any():

                df = df.with_column(
                    pl.when((pl.col(i).str.contains("0")) & (pl.col(i).is_str()))
                    .then(0)
                    .otherwise(pl.col(i))
                    .alias(i)
                )
                df = df.with_column(
                    pl.when((pl.col(i).str.contains("2")) & (pl.col(i).is_str()))
                    .then(1)
                    .otherwise(pl.col(i))
                    .alias(i)
                )
                df = df.with_column(
                    pl.when(pl.col(i) == "Y").then(1).when(pl.col(i) == "N").then(0).otherwise(pl.col(i)).alias(i)
                )

            elif df[i].is_in(["0", "1"]).any():

                df = df.with_column(
                    pl.when((pl.col(i).is_null()) | (pl.col(i).is_nan())).then(pl.lit(None)).otherwise(pl.col(i)).alias(i)
                )
                df = df.with_column(
                    pl.when(pl.col(i) == "1").then(1).when(pl.col(i) == "0").then(0).otherwise(pl.col(i)).alias(i)
                )
                df = df.with_column(
                    pl.when(pl.col(i) == "2").then(1).otherwise(pl.col(i)).alias(i)
                )


            df = df.with_column(pl.col(i).cast(pl.Int64))
        # handle case where the column is a boolean type
        elif colscheme[i].lower() == 'bit' and df.select(pl.col(i)).dtypes[0]==pl.Boolean:
            df = df.with_columns(pl.col(i).cast(pl.Int64).alias(i))
    return df
