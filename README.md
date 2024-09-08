# Tall table ingestion CLI script

## Overview

This Python script is designed to facilitate the ingestion of LDC talltable CSV files into a PostgreSQL database. It automates the process of loading CSV data, performing necessary data cleaning, and inserting the cleaned data into a database while handling potential issues such as duplicates and logging the entire process.


## Todo
- [x] Ordered ingestion: `dataHeader` first, followed by the rest of the tables
- [x] ForeignKey check on table creation
- [ ] projecttable: creation and ingestion
  - [ ] projectkey unique (no foreign key yet)
  - [ ] use excel file project_table column to replace whatever is on the dataframe already
  - [ ] stop ingestion with no projecttable entry, but if exists on projecttable use it for table
- [ ] reproduce primarykey scheme for non-dataheader/geoindicator tables
- [ ] how to avoid ingesting same data considering we're using rid's as pk and index

## To run
- Commands:
  - `python main.py`
  - `ingest`

## Project Structure

```bash
/simple-tall-ingester/
│
├── /data/              # Directory where tall table csvs are pulled from
├── config.py           # Configuration file for database and application settings
├── main.py             # Entry point for the script
├── /logs/              # Directory where log files are stored
│
├── /scripts/           # Directory containing core script modules
│   ├── data_loader.py  # Functions for loading CSV files into dataframes
│   ├── data_cleaner.py # Functions for cleaning and transforming data
│   ├── db_connector.py # Functions for database operations (table creation, data insertion)
│   ├── utils.py        # Functions not yet categorized in loading, cleaning, or ingesting
│   └── __init__.py     # Package initializer
│
├── /validation_schemas/ # Directory for schemaplan
└── /tests/             # Directory for test scripts (not yet implemented)

```

## Features

- **Configuration Management:**
  - All configurations are centralized in a single `config.py` file, making it easy to manage database connections, file paths, logging settings, and other application-specific parameters.

- **Data Loading:**
  - The script reads CSV files and loads them into a `polars` dataframe, ensuring efficient handling of large datasets.

- **Data Cleaning:**
  - Customizable data cleaning functions are provided to fix values, add necessary columns (e.g., a `DateLoadedInDb` column with the current date only populated on ingestion), and prepare the data for insertion into the database.

- **Database Integration:**
  - The script automatically creates a PostgreSQL table if it doesn't exist, including an auto-generated `rid` column as the primary key.
  - Duplicate handling is managed by enforcing unique constraints on specific columns or by deduplicating data at the dataframe level before insertion.
  - An index is created on the `rid` column to optimize query performance.

- **Logging:**
  - All operations, including data loading, cleaning, and database insertion, are logged.
  - Logs are stored in the `logs` directory, with detailed information about the execution process, errors, and exceptions.
  - The logging configuration can be easily adjusted via the `config.py` file to suit different environments (e.g., development, production).

- **Error Handling:**
  - The script includes comprehensive error handling to ensure robust operation, with all exceptions logged for easier debugging.

- **Validation:**
  - Validation using schemaplan of choice on both CSV loading and dataframe ingestion into DB.

