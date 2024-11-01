import os
from datetime import date
from dotenv import load_dotenv
import logging.config

load_dotenv()

DBSCHEMA = "public_test"
SCHEMAPLAN_PATH = "./validation_schemas/LDC_SchemaPlan_1.2.4.csv"
PROJECTFILE_PATH = "./data"
NOPRIMARYKEYPATH = "./noprimarykey"
DATABASE_CONFIG = {
    "dbname": os.getenv('PROD_DBNAME'),
    "user": os.getenv('PROD_DBUSER'),
    "password": os.getenv('PROD_DBPASSWORD'),
    "host": os.getenv('PROD_DBHOST'),
    "port": os.getenv('PROD_DBPORT'),
}

TODAYS_DATE = date.today().isoformat()
DATA_DIR = "./data" #change to tall?

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'file': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'filename': './logs/app.log',
            'formatter': 'detailed',
        },
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'simple',
        },
    },
    'formatters': {
        'detailed': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
        'simple': {
            'format': '%(levelname)s - %(message)s'
        },
    },
    'root': {
        'handlers': ['console', 'file'],
        'level': 'DEBUG',
    },
}

logging.config.dictConfig(LOGGING_CONFIG)
