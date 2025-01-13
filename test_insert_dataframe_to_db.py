import sys
import os
### debugging file

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from scripts.data_loader import process_csv
from scripts.db_connector import insert_dataframe_to_db
from config import DATA_DIR

# Mock DataFrame
project_key = None
data_dir = DATA_DIR
file_path = os.path.join(data_dir, "dataGap.csv") # change as needed
result = process_csv(file_path, project_key)
df = result['dataframe']
table_name = result['table_name']


# Call the function
insert_dataframe_to_db(df, table_name)
