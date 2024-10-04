import os
import json
import heapq
from datetime import datetime, timedelta
import time
from blocks_download import Config, EtherAPI, BlockProcessor, FileManager, BlockDownloader
from json.decoder import JSONDecodeError
import logging
from logger import LoggerConfig
  
logger = LoggerConfig().get_logger()

class BlockFileProcessor:
    def __init__(self, block_downloader, file_manager):
        self.block_downloader = block_downloader
        self.file_manager = file_manager    

    def load_block_data(self, json_file):
        try:
            block_data = self.file_manager.load_from_json(json_file)
            if not block_data:
                logger.warning(f"File {json_file} is empty or corrupted. Attempting to fetch missing data.")                
                block_number = int(json_file.split('_')[1].split('.')[0])   

                self.block_downloader.download_single_block(block_number, [])
                block_data = self.file_manager.load_from_json(json_file)

            return block_data

        except ValueError as e:
            logger.error(f"BlockFileProcessor.load_block_data - Error parsing block number from filename {json_file}: {e}")
            raise ValueError(f"Invalid block number format in {json_file}: {e}") from e

        except Exception as e:
            logger.exception(f"BlockFileProcessor.load_block_data - An unexpected error: {e}")
            raise RuntimeError(f"An unexpected error occurred: {e}") from e


class TransactionsGrouper:
    def __init__(self, block_file_processor):
        self.block_file_processor = block_file_processor        
        self.transactions_by_hour = {}
        self.config = Config()

    def group_transactions_by_hour(self, progress_callback=None, check_interrupt=None):        
        self.transactions_by_hour = {}
        total_files = len(self.config.JSON_FILES)
        processed_files = 0            

        logger.info(f"Starting transaction grouping. Total files to process: {total_files}")

        for json_file in self.config.JSON_FILES:
            try:            
                block_data = self.block_file_processor.load_block_data(json_file)

                if check_interrupt and check_interrupt():
                    logger.warning("Processing interrupted by user.")
                    break
                                
                block_timestamp = int(block_data["timestamp"])                    
                hour = datetime.utcfromtimestamp(block_timestamp).strftime("%Y-%m-%d %H:00:00")
                transactions = block_data['transactions']

                if hour not in self.transactions_by_hour:
                    self.transactions_by_hour[hour] = []
                self.transactions_by_hour[hour].extend(transactions)

                processed_files += 1
                progress_value = processed_files                
                if progress_callback:
                    progress_callback(total_files, progress_value) 

            except KeyError as e:
                logger.error(f"TransactionsGrouper.group_transactions_by_hour - Missing key in file {json_file}: {e}")
                raise KeyError(f"Missing field in file {json_file}: {e}") from e

            except ValueError as e:
                logger.error(f"TransactionsGrouper.group_transactions_by_hour - Invalid value in file {json_file}: {e}")
                raise ValueError(f"Invalid value in file {json_file}: {e}") from e

            except TypeError as e:
                logger.error(f"TransactionsGrouper.group_transactions_by_hour - Type error in file {json_file}: {e}")
                raise TypeError(f"Type error in file {json_file}: {e}") from e

            except Exception as e:
                logger.exception(f"TransactionsGrouper.group_transactions_by_hour - An unexpected error:  {e}")
                raise RuntimeError(f"An unexpected error: {e}") from e
         
        logger.info(f"Finished processing. Total files processed: {processed_files}/{total_files}")
        return self.transactions_by_hour


class TransactionProcessor:
    def __init__(self):
        self.total_transactions = 0
        self.total_fees = 0
        self.total_value_eth = 0

    def reset(self):        
        self.total_transactions = 0
        self.total_fees = 0.0        
        self.total_value_eth = 0.0
    
    def _summarize(self):
        summary = (f"Processing completed: Total transactions: {self.total_transactions},"
                   f"Total value: {self.total_value_eth} ETH, Total fees: {self.total_fees} ETH")

    def process_transaction(self, transaction):
        try:
            sender = transaction["from"]            
            receiver = transaction["to"]
            value_wei = int(transaction["value"], 16)
            gas_price_wei = int(transaction["gasPrice"], 16)
            gas_wei = int(transaction["gas"], 16)

            value_eth = value_wei / 10**18
            transaction_fee_wei = gas_price_wei * gas_wei
            transaction_fee_eth = transaction_fee_wei / 10**18

            self.total_transactions += 1
            self.total_fees += transaction_fee_eth
            self.total_value_eth += value_eth            

            self._summarize()

            return sender, receiver, value_eth

        except KeyError as e:
            logger.error(f"TransactionProcessor.process_transaction - Missing key in transaction data: {e}")
            raise KeyError(f"Missing transaction field: {e}") from e

        except ValueError as e:
            logger.error(f"TransactionProcessor.process_transaction - Invalid value in transaction data: {e}")
            raise ValueError(f"Invalid transaction value: {e}") from e

        except TypeError as e:
            logger.error(f"TransactionProcessor.process_transaction - Type error in transaction data: {e}")
            raise TypeError(f"Type error in transaction: {e}") from e
            
        except Exception as e:
            logger.exception(f"TransactionProcessor.process_transaction - Unexpected error during transaction processing: {e}")
            raise RuntimeError(f"An unexpected error occurred: {e}") from e


class WalletUpdater:
    def __init__(self):
        self.wallets_transactions = {}

    def reset(self):        
        self.wallets_transactions = {}
        logger.info("WalletUpdater.reset - Reset wallet transactions.")

    def update_wallets(self, sender, receiver, value_eth):
        try:            
            if sender not in self.wallets_transactions:
                self.wallets_transactions[sender] = []
            self.wallets_transactions[sender].append({"value": -value_eth, "type": "sell"})

            if receiver not in self.wallets_transactions:
                self.wallets_transactions[receiver] = []
            self.wallets_transactions[receiver].append({"value": value_eth, "type": "buy"})

        except TypeError as e:
            logger.error(f"WalletUpdater.update_wallets - Invalid type for value_eth: {value_eth}. Expected a float.")
            raise TypeError(f"Invalid type for value_eth: {e}")

        except Exception as e:            
            logger.exception("WalletUpdater.update_wallets - Unexpected error during wallet update.")
            raise Exception(f"An unexpected error occurred: {e}") from e
            

class WalletClassifier:
    @staticmethod
    def classify_wallet(balance):
        if balance >= 10000:
            return "Above 10000 ETH"
        elif balance >= 1000:
            return "1000-10000 ETH"
        elif balance >= 100:
            return "100-1000 ETH"
        elif balance >= 10:
            return "10-100 ETH"
        elif balance >= 1:
            return "1-10 ETH"
        elif balance >= 0.1:
            return "0.1-1 ETH"
        else:
            return "Below 0.1 ETH"

    def classify_wallets(self, wallets_transactions):        
        wallets_balances = {}
        try:
            for wallet, transactions in wallets_transactions.items():
                total_value_eth = sum(transaction["value"] for transaction in transactions)
                classification = self.classify_wallet(total_value_eth)
                if classification not in wallets_balances:
                    wallets_balances[classification] = 0
                wallets_balances[classification] += 1
        
            return wallets_balances        
        
        except KeyError as e:
            logger.error(f"WalletClassifier.classify_wallet - Missing key in transaction data: {e}")
            raise KeyError(f"Missing transaction field: {e}") from e

        except ValueError as e:
            logger.error(f"WalletClassifier.classify_wallet - Invalid value in transaction data: {e}")
            raise ValueError(f"Invalid transaction value: {e}") from e

        except TypeError as e:
            logger.error(f"WalletClassifier.classify_wallet - Type error in transaction data: {e}")
            raise TypeError(f"Type error in transaction: {e}") from e
            
        except AttributeError as e:
            logger.error(f"WalletClassifier.classify_wallet - Invalid data structure: {e}")
            raise AttributeError(f"Invalid data structure for wallets_transactions: {e}") from e

        except Exception as e:            
            logger.exception(f"WalletClassifier.classify_wallet - Unexpected error during wallet classification: {e}")
            raise RuntimeError(f"An unexpected error occurred: {e}") from e


class TopWalletsGenerator:
    def get_top_wallets(self, wallets_transactions, top_n=5, is_seller=False):
        try:
            key_func = (lambda x: -sum(t["value"] for t in x[1] if t["value"] < 0)) if is_seller else (lambda x: sum(t["value"] for t in x[1] if t["value"] > 0))
            top_wallets = heapq.nlargest(top_n, wallets_transactions.items(), key=key_func)

            top_wallets_info = []
            for wallet_info in top_wallets:
                wallet_address, transactions = wallet_info
                max_transaction_eth = min(transactions, key=lambda x: x["value"]) if is_seller else max(transactions, key=lambda x: x["value"])
                transaction_type = max_transaction_eth["type"]
                wallet_balance_eth = sum(transaction["value"] for transaction in transactions)
                wallet_info_with_balance = {
                    "wallet address": wallet_address,
                    "biggest transaction type (buy/sell)": transaction_type,
                    "biggest transaction amount in ether": max_transaction_eth["value"],
                    "wallet balance": wallet_balance_eth
                }
                top_wallets_info.append(wallet_info_with_balance)
            
            return top_wallets_info

        except KeyError as e:
            logger.error(f"TopWalletsGenerator.get_top_wallets - Missing key in transaction data: {e}")
            raise KeyError(f"Missing transaction field: {e}") from e

        except TypeError as e:
            logger.error(f"TopWalletsGenerator.get_top_wallets - Type error in transaction data: {e}")
            raise TypeError(f"Type error in transaction: {e}") from e
            
        except AttributeError as e:
            logger.error(f"TopWalletsGenerator.get_top_wallets - Invalid data structure: {e}")
            raise AttributeError(f"Invalid data structure for wallets_transactions: {e}") from e

        except Exception as e:            
            logger.exception(f"TopWalletsGenerator.get_top_wallets - Unexpected error!: {e}")
            raise RuntimeError(f"An unexpected error occurred: {e}") from e


class ResultFormatter:
    @staticmethod
    def format_result(start_hour_str, total_transactions, total_fees, wallets_balances, top_wallets_generator, wallets_transactions):
        try:
            average_fee_eth = total_fees / total_transactions if total_transactions > 0 else 0
            result_data = {
                "time": start_hour_str,
                "transactions number": total_transactions,
                "average transaction fee": average_fee_eth,
                "wallet classification in eth balance": wallets_balances,
                "top 5 buyers": top_wallets_generator.get_top_wallets(wallets_transactions, top_n=5, is_seller=False),
                "top 5 sellers": top_wallets_generator.get_top_wallets(wallets_transactions, top_n=5, is_seller=True)
            }

            return result_data

        except Exception as e:
            logger.exception(f"Unexpected error while formatting result: {e}")
            raise RuntimeError(f"An unexpected error occurred while formatting the result: {e}") from e


class HourlyDataExtractor:
    def __init__(self, transactions_grouper, transaction_processor, wallet_updater, wallet_classifier, top_wallets_generator, result_formatter):
        self.transactions_grouper = transactions_grouper
        self.transaction_processor = transaction_processor
        self.wallet_updater = wallet_updater
        self.wallet_classifier = wallet_classifier
        self.top_wallets_generator = top_wallets_generator
        self.result_formatter = result_formatter
        self.config = Config()

    def extract_data(self, extract_date, progress_callback=None, check_interrupt=None):
        transactions_by_hour = self.transactions_grouper.group_transactions_by_hour(progress_callback, check_interrupt)
        
        start_hour = datetime.strptime(extract_date, "%Y-%m-%d %H:%M:%S")
        hourly_results_all = []  
        current_hour = start_hour

        while current_hour.hour <= 23:
            end_hour_dt = current_hour + timedelta(hours=1)
            start_hour_str = current_hour.strftime("%Y-%m-%d %H:%M:%S")

            self.transaction_processor.reset()
            self.wallet_updater.reset()

            transactions_for_hour = transactions_by_hour.get(start_hour_str, [])

            print(current_hour, f"przetwarzana") 
            
            for transaction in transactions_for_hour:
                sender, receiver, value_eth = self.transaction_processor.process_transaction(transaction)
                self.wallet_updater.update_wallets(sender, receiver, value_eth)
            
            wallets_balances = self.wallet_classifier.classify_wallets(self.wallet_updater.wallets_transactions)
            
            result_data = self.result_formatter.format_result(
                start_hour_str,
                self.transaction_processor.total_transactions,
                self.transaction_processor.total_fees,
                wallets_balances,
                self.top_wallets_generator,
                self.wallet_updater.wallets_transactions
            )

            hourly_results_all.append(result_data)
            current_hour += timedelta(hours=1)

            print(current_hour, f"przetworzona")   
            if current_hour.date() != start_hour.date():
                break
        
        #todo
        date_part = start_hour.strftime("%Y-%m-%d")
        output_folder = "interesting_info"
        os.makedirs(output_folder, exist_ok=True)
        output_file_path = os.path.join(self.config.CURRENT_DIRECTORY, output_folder, f"{date_part}_hourly_data.json")
        with open(output_file_path, 'w') as output_file:
            json.dump(hourly_results_all, output_file, indent=4)


class DailyDataExtractor:
    def __init__(self, transactions_grouper, transaction_processor, wallet_updater, wallet_classifier, top_wallets_generator, result_formatter):
        self.transactions_grouper = transactions_grouper
        self.transaction_processor = transaction_processor
        self.wallet_updater = wallet_updater
        self.wallet_classifier = wallet_classifier
        self.top_wallets_generator = top_wallets_generator
        self.result_formatter = result_formatter
        self.config = Config()

    def extract_data(self, extract_date, progress_callback=None, check_interrupt=None):
        transactions_by_hour = self.transactions_grouper.group_transactions_by_hour(progress_callback, check_interrupt)
        transactions_for_day = []

        start_hour = datetime.strptime(extract_date, "%Y-%m-%d %H:%M:%S")
        start_hour_str = start_hour.strftime("%Y-%m-%d")

        for hour, transactions_in_hour in transactions_by_hour.items():
            hour_datetime = datetime.strptime(hour, "%Y-%m-%d %H:%M:%S")
            if hour_datetime.date() == start_hour.date():
                transactions_for_day.extend(transactions_in_hour)
        
        for transaction in transactions_for_day:
            sender, receiver, value_eth = self.transaction_processor.process_transaction(transaction)
            self.wallet_updater.update_wallets(sender, receiver, value_eth)
        
        wallets_balances = self.wallet_classifier.classify_wallets(self.wallet_updater.wallets_transactions)
        
        result_data = self.result_formatter.format_result(
            start_hour_str,
            self.transaction_processor.total_transactions,
            self.transaction_processor.total_fees,
            wallets_balances,
            self.top_wallets_generator,
            self.wallet_updater.wallets_transactions
        )

        #todo
        date_part = start_hour.strftime("%Y-%m-%d")    
        output_folder = "interesting_info"
        os.makedirs(output_folder, exist_ok=True)
        output_file_path = os.path.join(self.config.CURRENT_DIRECTORY, output_folder, f"{date_part}_daily_data.json")
        with open(output_file_path, 'w') as output_file:
            json.dump(result_data, output_file, indent=4)


if __name__ == "__main__":    
    config = Config()
    api = EtherAPI(config)
    file_manager = FileManager(config)
    block_downloader = BlockDownloader(api, file_manager)
    block_file_processor = BlockFileProcessor(block_downloader, file_manager)
    transactions_grouper = TransactionsGrouper(block_file_processor)
    transaction_processor = TransactionProcessor()
    wallet_updater = WalletUpdater()
    wallet_classifier = WalletClassifier()
    top_wallets_generator = TopWalletsGenerator()
    result_formatter = ResultFormatter()    
        
    print("Wybierz opcję:")
    print("1: Ekstrakcja danych godzinowych")
    print("2: Ekstrakcja danych dziennych")

    choice = input("Wybierz opcję (1/2): ")    
    extract_date = "2024-10-02 00:00:00"

    if choice == '1':
        hourly_extractor = HourlyDataExtractor(transactions_grouper, transaction_processor, wallet_updater, wallet_classifier, top_wallets_generator, result_formatter)
        hourly_extractor.extract_data(extract_date)
    elif choice == '2':
        daily_extractor = DailyDataExtractor(transactions_grouper, transaction_processor, wallet_updater, wallet_classifier, top_wallets_generator, result_formatter)
        daily_extractor.extract_data(extract_date)
    else:
        print("Nieznany tryb.")