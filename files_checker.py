import os
import json
from config import Config 
from database_tool import DatabaseManager
from logger import logger
from error_handler import ErrorHandler


@ErrorHandler.ehdc()
class FilesChecker:
    def __init__(self, config, database_manager):
        self.config = config
        self.database_manager = database_manager


    def ensure_directory(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
            logger.info(f'Directory "{path}" was successfully created.')


    def ensure_file(self, path, initializer=None):
        if not os.path.exists(path):
            with open(path, 'w') as f:
                if initializer:
                    initializer()
            logger.info(f'File "{path}" was successfully created.')


    def initialize_wallets_activity(self):
        with open(self.config.WALLETS_ACTIVITY_FILENAME, 'w') as json_file:
            json.dump({}, json_file)
        logger.info(f'File "{self.config.WALLETS_ACTIVITY_FILENAME}" initialized with an empty dictionary.')

    
    def initialize_database(self):
        with self.database_manager as db:
            db.execute_query('''
                CREATE TABLE IF NOT EXISTS combined_data (
                    id INTEGER PRIMARY KEY,
                    data_type TEXT,
                    date DATE,
                    hour INTEGER,
                    transactions_number INTEGER,
                    average_transaction_fee REAL,
                    wallet_0_1_eth INTEGER,
                    wallet_1_10_eth INTEGER,
                    wallet_10_100_eth INTEGER,
                    wallet_100_1000_eth INTEGER,
                    wallet_0_1_to_1_eth INTEGER,
                    wallet_above_10000_eth INTEGER,
                    wallet_1000_10000_eth INTEGER
                )
            ''')
            db.execute_query('''
                CREATE TABLE IF NOT EXISTS wallet_balance (
                    id INTEGER PRIMARY KEY,
                    wallet_address TEXT,
                    date DATE,
                    balance REAL,
                    last_update_date DATE,
                    top_buy_amount REAL,
                    top_buy_date DATE,
                    top_sell_amount REAL,
                    top_sell_date DATE,
                    UNIQUE(wallet_address, date)
                )
            ''')
        logger.info(f'Database "{self.config.DB_FILENAME}" initialized.')

    
    def check_files(self):
        logger.info("Checking files and directories...")
        
        self.ensure_directory(self.config.BLOCKS_DATA_DIR)
        self.ensure_directory(self.config.OUTPUT_FOLDER)
        self.ensure_directory(self.config.LOG_DIR)
        
        self.ensure_file(self.config.WALLETS_ACTIVITY_FILENAME, initializer=self.initialize_wallets_activity)
        self.ensure_file(self.config.LOG_FILE)
        self.ensure_file(self.config.DB_FILENAME, initializer=self.initialize_database)

        logger.info("All files and directories are checked!")

    
if __name__ == "__main__":
    input_file_name = "2024-10-02_daily_data.json"
    config = Config()
    database_manager = DatabaseManager(config.DB_FILENAME)
    files_checker = FilesChecker(config, database_manager)
    files_checker.check_files()