import os
import json
import heapq
from datetime import datetime, timedelta
import time
from blocks_download import Config, EtherAPI, BlockProcessor, FileManager, BlockDownloader
from json.decoder import JSONDecodeError
  

class BlockFileProcessor:
    def __init__(self, block_downloader, file_manager):
        self.block_downloader = block_downloader
        self.file_manager = file_manager
        
    def load_block_data(self, json_file):
        block_data = self.file_manager.load_from_json(json_file)
        if not block_data:
            print(f"Plik {json_file} jest pusty lub uszkodzony. Próba pobrania brakujących danych.")
            block_number = int(json_file.split('_')[1].split('.')[0])           
            fetched_block_numbers = []
            self.block_downloader.download_single_block(block_number, fetched_block_numbers)
            block_data = self.file_manager.load_from_json(json_file)
        
        return block_data


class TransactionsGrouper:
    def __init__(self, block_file_processor):
        self.block_file_processor = block_file_processor        
        self.transactions_by_hour = {}

    def group_transactions_by_hour(self, progress_callback=None, check_interrupt=None):
            transactions_by_hour = {}
            total_files = len(Config.JSON_FILES)
            processed_files = 0            

            for json_file in Config.JSON_FILES:
                block_data = self.block_file_processor.load_block_data(json_file)

                if check_interrupt and check_interrupt():
                    print('rozwaloned')
                    break

                processed_files += 1
                progress_value = processed_files                
                if progress_callback:
                    progress_callback(total_files, progress_value) 
                
                block_timestamp = int(block_data["timestamp"])
                hour = datetime.utcfromtimestamp(block_timestamp).strftime("%Y-%m-%d %H:00:00")
                transactions = block_data["transactions"]
                if hour not in self.transactions_by_hour:
                    self.transactions_by_hour[hour] = []
                self.transactions_by_hour[hour].extend(transactions)

            return self.transactions_by_hour
    

class TransactionProcessor:
    def __init__(self):
        self.total_transactions = 0
        self.total_fees = 0
        self.wallets_transactions = {}
        self.wallets_balances = {}
    
    def process_transactions(self, transactions_for_hour):       
        
        for transaction in transactions_for_hour:        
            
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

            self._update_wallets(sender, receiver, value_eth)
            print(f"Przetworzona transakcja: od {sender} do {receiver} na kwotę {value_eth} ETH.")
    
    def _update_wallets(self, sender, receiver, value_eth):
        if sender not in self.wallets_transactions:
            self.wallets_transactions[sender] = []
        self.wallets_transactions[sender].append({"value": -value_eth, "type": "sell"})

        if receiver not in self.wallets_transactions:
            self.wallets_transactions[receiver] = []
        self.wallets_transactions[receiver].append({"value": value_eth, "type": "buy"})

    def classify_wallets(self):
        for wallet, transactions in self.wallets_transactions.items():
            total_value_eth = sum(transaction["value"] for transaction in transactions)
            classification = self.classify_wallet(total_value_eth)
            if classification not in self.wallets_balances:
                self.wallets_balances[classification] = 0
            self.wallets_balances[classification] += 1

    def classify_wallet(self, balance):
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

    def get_top_wallets(self, top_n=5, is_seller=False):
        key_func = (lambda x: -sum(t["value"] for t in x[1] if t["value"] < 0)) if is_seller else (lambda x: sum(t["value"] for t in x[1] if t["value"] > 0))
        top_wallets = heapq.nlargest(top_n, self.wallets_transactions.items(), key=key_func)
        
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

    def generate_result_data(self, start_hour_str, hourly_mode, hourly_results_all, current_hour):
        average_fee_eth = self.total_fees / self.total_transactions if self.total_transactions > 0 else 0
        result_data = {
            "time": start_hour_str,
            "transactions number": self.total_transactions,
            "average transaction fee": average_fee_eth,
            "wallet classification in eth balance": self.wallets_balances,
            "top 5 buyers": self.get_top_wallets(top_n=5, is_seller=False),
            "top 5 sellers": self.get_top_wallets(top_n=5, is_seller=True)
        }       
        print(f"Generowanie danych dla godziny: {start_hour_str}, liczba transakcji: {self.total_transactions}")

        if hourly_mode:
            hourly_results_all.append(result_data)
            current_hour += timedelta(hours=1)
            return current_hour, self.wallets_transactions, self.wallets_balances
        else:
            return result_data


def extract_hourly_data(extract_date, progress_callback=None, check_interrupt=None):            
    config = Config()
    api = EtherAPI(config)    
    file_manager = FileManager(config)
    block_downloader = BlockDownloader(api, file_manager)    
    block_file_processor = BlockFileProcessor(block_downloader, file_manager)
    
    transactions_grouper = TransactionsGrouper(block_file_processor)
    transactions_by_hour = transactions_grouper.group_transactions_by_hour(progress_callback, check_interrupt=check_interrupt)   
    
    start_hour = datetime.strptime(extract_date, "%Y-%m-%d %H:%M:%S")
    hourly_results_all = []  
    current_hour = start_hour
    hourly_mode = True

    def process_and_save_transactions(transactions_for_hour, start_hour_str, end_hour_str, hourly_mode, hourly_results_all, current_hour):
        return processor.generate_result_data(start_hour_str, hourly_mode, hourly_results_all, current_hour)

    while current_hour.hour <= 23:
        end_hour_dt = current_hour + timedelta(hours=1)

        start_hour_str = current_hour.strftime("%Y-%m-%d %H:%M:%S")
        end_hour_str = end_hour_dt.strftime("%Y-%m-%d %H:%M:%S")

        transactions_for_hour = transactions_by_hour.get(start_hour_str, [])

        wallets_transactions = {}        
        wallets_balances = {
                                "Above 10000 ETH": 0,
                                "1000-10000 ETH": 0,
                                "100-1000 ETH": 0,
                                "10-100 ETH": 0,
                                "1-10 ETH": 0,
                                "0.1-1 ETH": 0,
                                "0.1 ETH": 0
                            }

        total_transactions = 0
        total_fees = 0
        
        print(current_hour, f"przetwarzana") 

        current_hour, wallets_transactions, wallets_balances = process_and_save_transactions(transactions_by_hour, start_hour_str, end_hour_str, hourly_mode, hourly_results_all, current_hour)
        
        print(current_hour, f"przetworzona")   
        if current_hour.date() != start_hour.date():
            break

    date_part = start_hour.strftime("%Y-%m-%d")
    output_folder = "interesting_info"
    os.makedirs(output_folder, exist_ok=True)
    output_file_path = os.path.join(Config.CURRENT_DIRECTORY, output_folder, f"{date_part}_hourly_data.json")
    with open(output_file_path, 'w') as output_file:
        json.dump(hourly_results_all, output_file, indent=4)


def extract_daily_data(extract_date, progress_callback=None, check_interrupt=None):   
    config = Config()
    api = EtherAPI(config)
    file_manager = FileManager(config)
    block_downloader = BlockDownloader(api, file_manager)    
    block_file_processor = BlockFileProcessor(block_downloader, file_manager)
 
    transactions_grouper = TransactionsGrouper(block_file_processor)
    transactions_by_hour = transactions_grouper.group_transactions_by_hour(progress_callback, check_interrupt=check_interrupt)   
    transactions_for_day = []


    def process_and_save_transactions(transactions_for_hour, start_hour_str, end_hour_str, hourly_mode, hourly_results_all, current_hour):
        # Utwórz instancję TransactionProcessor
        processor = TransactionProcessor()        
        # Przetwórz transakcje dla bieżącej godziny
        processor.process_transactions(transactions_for_hour)        
        # Skategoryzuj portfele
        processor.classify_wallets()        
        # Generuj dane wynikowe
        result_data = processor.generate_result_data(start_hour_str, hourly_mode, hourly_results_all, current_hour)
        
        return result_data


    start_hour = datetime.strptime(extract_date, "%Y-%m-%d %H:%M:%S")
    end_hour = datetime.strptime(extract_date, "%Y-%m-%d %H:%M:%S")
    start_hour_str = start_hour.strftime("%Y-%m-%d")
    end_hour_str = end_hour.strftime("%Y-%m-%d")

    for hour, transactions_in_hour in transactions_by_hour.items():
        hour_datetime = datetime.strptime(hour, "%Y-%m-%d %H:%M:%S")
        if hour_datetime.date() == start_hour.date():
            transactions_for_day.extend(transactions_in_hour)

    hourly_results_all = None
    current_hour = None

    wallets_transactions = {}    
    wallets_balances = {
                            "Above 10000 ETH": 0,
                            "1000-10000 ETH": 0,
                            "100-1000 ETH": 0,
                            "10-100 ETH": 0,
                            "1-10 ETH": 0,
                            "0.1-1 ETH": 0,
                            "0.1 ETH": 0
                        }

    total_transactions = 0
    total_fees = 0
             
    hourly_mode = False   
    

    result_data = process_and_save_transactions(transactions_for_day, start_hour_str, end_hour_str, hourly_mode, hourly_results_all, current_hour)

    date_part = start_hour.strftime("%Y-%m-%d")    
    output_folder = "interesting_info"
    os.makedirs(output_folder, exist_ok=True)
    output_file_path = os.path.join(Config.CURRENT_DIRECTORY, output_folder, f"{date_part}_daily_data.json")
    with open(output_file_path, 'w') as output_file:
        json.dump(result_data, output_file, indent=4)   