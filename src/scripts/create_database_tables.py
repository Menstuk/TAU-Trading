import logging
import sys
from utils import models, models_for_preprocessing_test as models_for_pp
from utils.custom_log_formatter import CustomFormatter

if __name__ == '__main__':
    # initialize logger
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
    logging.root.handlers[0].setFormatter(CustomFormatter())

    logging.info("Creating database's tables...")
    # create db tables if they don't already exist in database
    models.create_tables_for_all_models()

    # uncomment the code below to create db tables with null values - if they don't already exist in database
    # models_for_pp.create_tables_for_all_models_with_null()
    print("process finished")

