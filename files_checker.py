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
            if initializer:
                initializer()
        logger.info(f'File "{path}" was successfully created.')

    def initialize_env_file(self):
        with open(".env", "w") as f:
            f.write(
                "APP_BASE_DIR=\n"
                "API_KEY=\n"
                "API_URL=https://api.etherscan.io/api\n"
                "REQUEST_DELAY=0.25\n"
            )
        logger.info(".env file created with default values.")


    def ensure_env_file(self):
        api_key = self.config.API_KEY

        if not api_key or api_key.strip() == "":
            message = "API KEY missing! Add it to .env file and restart application."
            logger.warning(message)
            print(f"WARNING: {message}")


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

        self.ensure_file('.env', initializer=self.initialize_env_file)

        logger.info("All files and directories are checked!")


class FilesCheckerFactory:
    @staticmethod
    def create_files_checker(config):
        database_manager = DatabaseManager(config.DB_FILENAME)
        files_checker_instance = FilesChecker(config, database_manager)
        return files_checker_instance


if __name__ == "__main__":
    input_file_name = "2024-10-02_daily_data.json"
    config_instance = Config()
    files_checker = FilesCheckerFactory.create_files_checker(config_instance)
    files_checker.check_files()