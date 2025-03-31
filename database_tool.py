import sqlite3
import json
from datetime import datetime
import os
from config import Config
from error_handler import ErrorHandler
from logger import logger


@ErrorHandler.ehdc()
class DatabaseManager:
    def __init__(self, db_filename):
        self.db_filename = db_filename
        self.connection = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def connect(self):         
        self.connection = sqlite3.connect(self.db_filename)
        logger.info(f"Connected to database '{self.db_filename}' successfully.")
    
    def disconnect(self):        
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info(f"Disconnected from database '{self.db_filename}'.")

    def execute_query(self, query, parameters=None):        
        cursor = self.connection.cursor()
        if parameters:
            cursor.execute(query, parameters)
            logger.debug(f"Executed query with parameters: {query}, {parameters}")

        else:
            cursor.execute(query)
            logger.debug(f"Executed query: {query}")
        
        self.connection.commit()
        logger.debug(f"Query committed successfully.")
        return cursor


@ErrorHandler.ehdc()
class DataCalculator:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def table_data_calculations(self, entry, data_type):
        with self.db_manager as db:            
                date = entry.get('time')
                transactions_number = entry.get('transactions number')
                average_transaction_fee = entry.get('average transaction fee')
                wallet_classification = entry.get('wallet classification in eth balance', {})

                wallet_keys = [
                    'Below 0.1 ETH', '1-10 ETH', '10-100 ETH', '100-1000 ETH', 
                    '0.1-1 ETH', 'Above 10000 ETH', '1000-10000 ETH'
                ]
                wallet_values = [wallet_classification.get(key, 0) for key in wallet_keys]

                hour = None
                if data_type == 'hourly':
                    hour_str = entry.get('time')
                    hour_dt = datetime.strptime(hour_str, "%Y-%m-%d %H:%M:%S")
                    hour = hour_dt.hour

                select_query = 'SELECT * FROM combined_data WHERE date = ?'
                if data_type == 'hourly':
                    select_query += ' AND hour = ?'
                    
                select_params = (date,) if data_type == 'daily' else (date, hour)

                existing_entry = db.execute_query(select_query, select_params).fetchone()

                if existing_entry:
                    logger.info(f"Entry for date {date} and hour {hour} already exists.")
                else:
                    insert_query = '''
                        INSERT INTO combined_data (
                            data_type, date, hour, transactions_number, average_transaction_fee,
                            wallet_0_1_eth, wallet_1_10_eth, wallet_10_100_eth,
                            wallet_100_1000_eth, wallet_0_1_to_1_eth, wallet_above_10000_eth,
                            wallet_1000_10000_eth
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    '''
                    insert_params = (
                        data_type, date, hour, transactions_number, average_transaction_fee,
                        *wallet_values
                    )
                    db.execute_query(insert_query, insert_params)


@ErrorHandler.ehdc()
class DataImporter:
    def __init__(self, config, db_manager, data_calculator):
        self.config = config
        self.db_manager = db_manager
        self.data_calculator = data_calculator

    def import_data_to_combined_table(self, input_file_name, data_type):
            input_file_path = os.path.join(self.config.BASE_DIR, "interesting_info", input_file_name)

            with open(input_file_path, "r") as file:
                data = json.load(file)

            if data_type == "hourly":
                for entry in data:
                    self.data_calculator.table_data_calculations(entry, data_type)

            if data_type == "daily":        
                self.data_calculator.table_data_calculations(data, data_type)


@ErrorHandler.ehdc()
class BiggestWalletsData:
    def __init__(self, config, db_manager):
        self.config = config
        self.db_manager = db_manager        

    def save_biggest_wallets_activity_database(self, input_file_name):
        input_file_path = os.path.join(self.config.BASE_DIR, "interesting_info", input_file_name)

        with self.db_manager as db:                     
                with open(input_file_path, "r") as file:
                    wallet_data = json.load(file)

                for wallet_address, wallet_info in wallet_data.items():
                    if isinstance(wallet_info, dict):
                        balance_history = wallet_info.get('balance_history', [])

                        top_buy_transaction = wallet_info.get('top_buy_transaction', {})
                        top_sell_transaction = wallet_info.get('top_sell_transaction', {})

                        top_buy_amount = top_buy_transaction.get('amount')
                        top_buy_date_str = top_buy_transaction.get('date')
                        top_buy_date = datetime.strptime(top_buy_date_str, "%Y-%m-%d").date() if top_buy_date_str else None

                        top_sell_amount = top_sell_transaction.get('amount')
                        top_sell_date_str = top_sell_transaction.get('date')
                        top_sell_date = datetime.strptime(top_sell_date_str, "%Y-%m-%d").date() if top_sell_date_str else None
                        
                        for entry in balance_history:
                            date_str = entry['date']
                            balance = entry['balance']
                            datetime_obj = datetime.strptime(date_str, "%Y-%m-%d")
                            date = datetime_obj.date()

                            last_update_date_str = datetime.now().strftime("%Y-%m-%d")
                            last_update_date = datetime.strptime(last_update_date_str, "%Y-%m-%d").date()

                            select_query = 'SELECT * FROM wallet_balance WHERE wallet_address = ? AND date = ? AND balance = ?'        
                                
                            select_params = (wallet_address, date, balance)

                            existing_entry = db.execute_query(select_query, select_params).fetchone()                            


                            if existing_entry:
                                print(f"Entry for wallet {wallet_address}, date {date}, and balance {balance} already exists.")
                            else:
                                db.execute_query('''
                                    INSERT OR REPLACE INTO wallet_balance (
                                        wallet_address, date, balance,
                                        last_update_date, top_buy_amount, top_buy_date,
                                        top_sell_amount, top_sell_date
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                ''', (
                                    wallet_address, date, balance,
                                    last_update_date, top_buy_amount, top_buy_date,
                                    top_sell_amount, top_sell_date
                                ))
                

@ErrorHandler.ehdc()
class DataChecker:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def check_date_in_database(self, selected_date, sql_query_check):
            with self.db_manager as db:    
                cursor = db.execute_query(sql_query_check, (selected_date.strftime("%Y-%m-%d"),))                
                result = cursor.fetchone()
                print(result)
                return bool(result)


@ErrorHandler.ehdc()
class DataCleaner:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def remove_invalid_entries(self):
        with self.db_manager as db:        
                delete_query = '''
                    DELETE FROM combined_data
                    WHERE (transactions_number IS NULL OR transactions_number = 0)
                    AND (average_transaction_fee IS NULL OR average_transaction_fee = 0)
                    AND (wallet_0_1_eth = 0)
                    AND (wallet_1_10_eth = 0)
                    AND (wallet_10_100_eth = 0)
                    AND (wallet_100_1000_eth = 0)
                    AND (wallet_0_1_to_1_eth = 0)
                    AND (wallet_above_10000_eth = 0)
                    AND (wallet_1000_10000_eth = 0)
                '''

                db.execute_query(delete_query)


@ErrorHandler.ehdc()
class DataReader:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def read_and_display_data_from_database(self):
        with self.db_manager as db:                  
                db.connection.row_factory = sqlite3.Row 
                rows = db.execute_query('SELECT * FROM wallet_balance').fetchall()

                for row in rows:
                    wallet_address = row['wallet_address']
                    date = row['date']
                    balance = row['balance']
                    last_update_date = row['last_update_date']
                    top_buy_amount = row['top_buy_amount']
                    top_buy_date = row['top_buy_date']
                    top_sell_amount = row['top_sell_amount']
                    top_sell_date = row['top_sell_date']

                    print("Wallet Address:", wallet_address)
                    print("Date:", date)
                    print("Balance:", balance)
                    print("Last Update Date:", last_update_date)
                    print("Top Buy Amount:", top_buy_amount)
                    print("Top Buy Date:", top_buy_date)
                    print("Top Sell Amount:", top_sell_amount)
                    print("Top Sell Date:", top_sell_date)
                    print("\n")


@ErrorHandler.ehdc()
class DataDisplay:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def print_combined_data_by_type(self, data_type):
        with self.db_manager as db:
                rows = db.execute_query('SELECT * FROM combined_data WHERE data_type = ?', (data_type,)).fetchall()
                for row in rows:
                    print("Data Type:", row[1])
                    print("Date:", row[2])
                    print("Hour:", row[3])
                    print("Transactions Number:", row[4])
                    print("Average Transaction Fee:", row[5])
                    print("Wallet Classification:")
                    print("  0.1 ETH:", row[6])
                    print("  1-10 ETH:", row[7])
                    print("  10-100 ETH:", row[8])
                    print("  100-1000 ETH:", row[9])
                    print("  0.1-1 ETH:", row[10])
                    print("  Above 10000 ETH:", row[11])
                    print("  1000-10000 ETH:", row[12])
                    print("\n")


@ErrorHandler.ehdc()
class DatabaseFactory:
    @staticmethod
    def create_database_components(config: Config = None):
        config = config or Config()
        db_filename = config.DB_FILENAME
        database_manager = DatabaseManager(db_filename)
        data_calculator = DataCalculator(database_manager)

        return {
            "database_manager": database_manager,
            "data_reader": DataReader(database_manager),
            "data_display": DataDisplay(database_manager),
            "data_checker": DataChecker(database_manager),
            "data_calculator": data_calculator,
            "data_importer": DataImporter(config, database_manager, data_calculator),
            "data_cleaner": DataCleaner(database_manager),
            "save_biggest_wallets": BiggestWalletsData(config, database_manager),
        }


if __name__ == "__main__":
    """
    mainly for testing    
    """
    config = Config()
    db_filename = config.DB_FILENAME
    database_manager = DatabaseManager(db_filename)
    data_reader = DataReader(database_manager)
    data_display = DataDisplay(database_manager)
    data_checker = DataChecker(database_manager)    
    data_calculator = DataCalculator(database_manager)
    data_importer = DataImporter(config, database_manager, data_calculator)
    data_cleaner = DataCleaner(database_manager)
    data_type =  "daily"
    input_file_name = "2024-10-02_daily_data.json"

    data_display.print_combined_data_by_type(data_type="daily")