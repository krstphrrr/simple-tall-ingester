from scripts.utils import process_csv
from config import DATA_DIR
import logging

logger = logging.getLogger(__name__)


class TallIngester(cmd.Cmd):
    intro = 'Welcome to the tall table ingester. Type help or ? to list commands.\n'
    prompt = '(tall-table-ingester) '

    def __init__(self):
        super().__init__()



    def do_ingest(self, arg):
        data_dir = DATA_DIR

        for file_name in os.listdir(data_dir):
            file_path = os.path.join(data_dir, file_name)

            # Check if the file is a CSV
            if file_name.endswith(".csv"):
                process_csv(file_name, file_path)
            else:
                logger.info(f"Skipping non-CSV file: {file_name}")


    def do_exit(self, arg):
        'Exit the CLI'
        print('Exiting the CLI.')
        return True

if __name__ == '__main__':
    TallIngester().cmdloop()
