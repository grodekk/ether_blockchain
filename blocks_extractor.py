import os
import json
import heapq
from datetime import datetime, timedelta
import time
from blocks_download import download_single_block
from json.decoder import JSONDecodeError

current_directory = os.path.dirname(__file__)
input_folder = os.path.join(current_directory, 'blocks_data')

def extract_block_data(file_path):
    with open(file_path, 'r') as file:
        block_data = json.load(file)
    return block_data


def group_transactions_by_hour(input_folder, json_files, progress_callback=None, check_interrupt=None):

        transactions_by_hour = {}

        total_files = len(json_files)
        processed_files = 0

        for json_file in json_files:
            try:
                with open(os.path.join(input_folder, json_file), 'r') as file:
                    print(f'funkcja gtbh - processing{json_file}')
                    block_data = json.load(file)

            except JSONDecodeError:
                    print(f"Plik {json_file} jest pusty lub uszkodzony. Próba pobrania brakujących danych.")                    
                    block_number = int(json_file.split('_')[1].split('.')[0])
                    fetched_block_numbers = []
                    download_single_block(block_number, fetched_block_numbers)               
                
                    with open(os.path.join(input_folder, json_file), 'r') as file:
                        block_data = json.load(file)

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
            if hour not in transactions_by_hour:
                transactions_by_hour[hour] = []
            transactions_by_hour[hour].extend(transactions)

        return transactions_by_hour
   

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
        return "0.1 ETH"


def process_and_save_transactions(transactions_for_hour,
                                  start_hour_str,
                                  end_hour_str,
                                  total_transactions,
                                  total_fees,
                                  wallets_transactions,
                                  wallets_balances,
                                  hourly_mode,
                                  hourly_results_all,
                                  current_hour):
            
        for transaction in transactions_for_hour:
                                                    
                            sender = transaction["from"]
                            receiver = transaction["to"]
                            value_wei = int(transaction["value"], 16)
                            gas_price_wei = int(transaction["gasPrice"], 16)
                            gas_wei = int(transaction["gas"], 16)  

                            value_eth = value_wei / 10**18

                            transaction_fee_wei = gas_price_wei * gas_wei
                            transaction_fee_eth = transaction_fee_wei / 10**18
                            
                            total_transactions += 1
                            total_fees += transaction_fee_eth

                            wallets_transactions.setdefault(sender, [])
                            wallets_transactions[sender].append({"value": -value_eth, "type": "sell"})

                            wallets_transactions.setdefault(receiver, [])
                            wallets_transactions[receiver].append({"value": value_eth, "type": "buy"})


        for wallet, transactions in wallets_transactions.items():
            total_value_eth = sum(transaction["value"] for transaction in transactions)  
            classification = classify_wallet(total_value_eth)
            wallets_balances.setdefault(classification, 0) 
            wallets_balances[classification] += 1


        if total_transactions != 0:
             srednia_oplata_eth = total_fees / total_transactions
        else:
            print("zero transakcji w tej godzinie")
            srednia_oplata_eth = 0 
        
        
        result_data = {
                "time": start_hour_str,
                "transactions number": total_transactions,
                "average transaction fee": srednia_oplata_eth,
                "wallet classification in eth balance": wallets_balances,
                "top 5 buyers": [],
                "top 5 sellers": []
            }
    

        top_5_buyers = heapq.nlargest(5, wallets_transactions.items(), key=lambda x: sum(t["value"] for t in x[1] if t["value"] > 0))

        for wallet_info in top_5_buyers:
                wallet_address, transactions = wallet_info
                max_transaction_eth = max(transactions, key=lambda x: x["value"])
                transaction_type = max_transaction_eth["type"]
                wallet_balance_eth = sum(transaction["value"] for transaction in transactions) 
                wallet_info_with_balance = {
                    "wallet adress": wallet_address,
                    "biggest transaction type(buy/sell)": transaction_type,
                    "biggest transaction amount in ether": max_transaction_eth["value"],
                    "wallet balance": wallet_balance_eth
                }
                result_data["top 5 buyers"].append(wallet_info_with_balance)
            

        top_5_sellers = heapq.nlargest(5, wallets_transactions.items(), key=lambda x: -sum(t["value"] for t in x[1] if t["value"] < 0))

        for wallet_info in top_5_sellers:
                wallet_address, transactions = wallet_info
                max_transaction_eth = min(transactions, key=lambda x: x["value"])
                transaction_type = max_transaction_eth["type"]
                wallet_balance_eth = sum(transaction["value"] for transaction in transactions)  
                wallet_info_with_balance = {
                    "wallet adress": wallet_address,
                    "biggest transaction type(buy/sell)": transaction_type,
                    "biggest transaction amount in ether": max_transaction_eth["value"],
                    "wallet balance": wallet_balance_eth
                }
                result_data["top 5 sellers"].append(wallet_info_with_balance)


        if hourly_mode:
            hourly_results_all.append(result_data)
            current_hour = current_hour + timedelta(hours=1)            
            return current_hour, wallets_transactions, wallets_balances
        
        else:
            return result_data


def extract_hourly_data(extract_date, progress_callback=None, check_interrupt=None):

    input_folder = os.path.join(current_directory, 'blocks_data')      
    json_files = [file for file in os.listdir(input_folder) if file.endswith(".json")]  

    transactions_by_hour = group_transactions_by_hour(input_folder, json_files, progress_callback, check_interrupt=check_interrupt)     
    start_hour = datetime.strptime(extract_date, "%Y-%m-%d %H:%M:%S")
    hourly_results_all = []  
    current_hour = start_hour
    hourly_mode = True


    while current_hour.hour <= 23:
        end_hour_dt = current_hour + timedelta(hours=1)

        start_hour_str = current_hour.strftime("%Y-%m-%d %H:%M:%S")
        end_hour_str = end_hour_dt.strftime("%Y-%m-%d %H:%M:%S")

        transactions_for_hour = transactions_by_hour.get(start_hour_str, [])

        wallets_transactions = {}
        wallets_balances = {classify_wallet(i): 0 for i in range(8)}
        total_transactions = 0
        total_fees = 0
        
        print(current_hour, f"przetwarzana") 

        current_hour, wallets_transactions, wallets_balances = process_and_save_transactions(transactions_for_hour, start_hour_str, end_hour_str, total_transactions,
        total_fees, wallets_transactions, wallets_balances, hourly_mode, hourly_results_all, current_hour)
        
        print(current_hour, f"przetworzona")   
        if current_hour.date() != start_hour.date():
            break

    date_part = start_hour.strftime("%Y-%m-%d")
    output_folder = "interesting_info"
    os.makedirs(output_folder, exist_ok=True)
    output_file_path = os.path.join(current_directory, output_folder, f"{date_part}_hourly_data.json")
    with open(output_file_path, 'w') as output_file:
        json.dump(hourly_results_all, output_file, indent=4)


def extract_daily_data(extract_date, progress_callback=None, check_interrupt=None):
    input_folder = os.path.join(current_directory, 'blocks_data')
    json_files = [file for file in os.listdir(input_folder) if file.endswith(".json")]

    transactions_by_hour = group_transactions_by_hour(input_folder, json_files, progress_callback, check_interrupt=check_interrupt) 
    transactions_for_day = []

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
    wallets_balances = {classify_wallet(i): 0 for i in range(8)}
    total_transactions = 0
    total_fees = 0
             
    hourly_mode = False    
    
    result_data = process_and_save_transactions(transactions_for_day, start_hour_str, end_hour_str, total_transactions, total_fees, wallets_transactions,
    wallets_balances, hourly_mode, hourly_results_all, current_hour)

    date_part = start_hour.strftime("%Y-%m-%d")    
    output_folder = "interesting_info"
    os.makedirs(output_folder, exist_ok=True)
    output_file_path = os.path.join(current_directory, output_folder, f"{date_part}_daily_data.json")
    with open(output_file_path, 'w') as output_file:
        json.dump(result_data, output_file, indent=4)   