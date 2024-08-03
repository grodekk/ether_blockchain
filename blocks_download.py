from multiprocessing import cpu_count, Manager, Pool, Lock
import threading
from queue import Empty
import time
import requests 
import json
import os
from functools import partial
from PyQt5.QtWidgets import QInputDialog
from PyQt5.QtCore import pyqtSignal, QObject
from PyQt5.QtCore import QDate, pyqtSignal, QThread, QMetaObject 
from PyQt5.QtCore import QTimer, QObject, QPropertyAnimation, QEasingCurve, QRect, pyqtProperty, pyqtSlot
from PyQt5.QtCore import QDate, pyqtSignal, QThread, QMetaObject 
import asyncio
from datetime import datetime, timezone, timedelta

API_KEY = "ZA1WF2Z9ZFJBGWPI8N4C6F6ARUVD3K7K5E"
API_URL = "https://api.etherscan.io/api"

REQUEST_DELAY = 0.2
current_directory = os.path.dirname(__file__)
folder_path = os.path.join(current_directory, 'blocks_data')
BLOCKS_DATA_FILE = os.path.join(current_directory, 'blocks_data.json')
progress_lock = Lock()

def get_timestamp_of_first_block_on_target_date(target_date):    
    target_timestamp = int(datetime.strptime(target_date + " 00:00", "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc).timestamp())    
    latest_block_number = get_latest_block_number()    
    start_block_number = latest_block_number
    end_block_number = 0
    
    while start_block_number > end_block_number:            

        mid_block_number = (start_block_number + end_block_number) // 2
        mid_block_timestamp = get_block_timestamp(hex(mid_block_number))        
        print(f"Checking block number: {mid_block_number}, Timestamp: {mid_block_timestamp}")
        
        if mid_block_timestamp > target_timestamp:
            start_block_number = mid_block_number - 1        
        else:
            end_block_number = mid_block_number + 1
    
    final_block_timestamp = get_block_timestamp(hex(start_block_number))
    if final_block_timestamp < target_timestamp:
        start_block_number += 1
    
    return start_block_number


def get_timestamp_of_last_block_on_target_date(target_date):   
    next_day = datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=1)
    target_timestamp = int(next_day.replace(tzinfo=timezone.utc).timestamp())    
    latest_block_number = get_latest_block_number()    
    start_block_number = latest_block_number
    end_block_number = 0

    while start_block_number > end_block_number:       
        mid_block_number = (start_block_number + end_block_number) // 2
        mid_block_timestamp = get_block_timestamp(hex(mid_block_number))        
        print(f"Checking block number: {mid_block_number}, Timestamp: {mid_block_timestamp}")
        
        if mid_block_timestamp >= target_timestamp:
            start_block_number = mid_block_number - 1     
        else:
            end_block_number = mid_block_number + 1
   
    final_block_timestamp = get_block_timestamp(hex(start_block_number))
    if final_block_timestamp >= target_timestamp:
        start_block_number -= 1

    last_block_number = start_block_number

    return last_block_number


def get_num_blocks_to_fetch(method="console"):
    if method == "console":
        return int(input("Enter the number of recent blocks to fetch: ")), True
        
    elif method == "interface":        
        num_blocks, ok_pressed = QInputDialog.getInt(None, "Ilość bloków", "Wprowadź ilość bloków do pobrania:")

        if ok_pressed:
            return num_blocks, True
        else:
            return None, False
    else:
        raise ValueError("Invalid method. Use 'console' or 'interface'.")


def process_block(block_number, result_queue, fetched_block_numbers, interrupt_flag=False):

    print(f'process block start flag = {interrupt_flag}')
    try:     
        if interrupt_flag.value:
            print(f"Przerwanie wykryte w bloku: {block_number}. Zakończono.")
            return
        
        print("Processing block:", block_number)
        
        if block_number in fetched_block_numbers:
            print(f"Block {block_number} already fetched. Skipping...")
            return None        

        timestamp = get_block_timestamp(hex(block_number))
        if timestamp == 0:
            return None

        transactions = get_block_transactions(hex(block_number))
        block_data = {
            "block_number": block_number,
            "timestamp": timestamp,
            "transactions": transactions
        }


        file_path = os.path.join(folder_path, f"block_{block_number}.json")       
        save_block_data_to_json(block_data, file_path)

        time.sleep(REQUEST_DELAY)
        result_queue.put(block_number) 

        # Aktualizuj postęp
        return 1 

    except Exception as e:
        print(f"Exception occurred in process_block for block {block_number}: {str(e)}")
        return None
   

def download_single_block(block_number, fetched_block_numbers):
    print("Downloading single block:", block_number)
    if block_number in fetched_block_numbers:
        print(f"Block {block_number} already fetched. Skipping...")
        return

    timestamp = get_block_timestamp(hex(block_number))
    if timestamp == 0:
        return

    transactions = get_block_transactions(hex(block_number))
    block_data = {
        "block_number": block_number,
        "timestamp": timestamp,
        "transactions": transactions
    }

    file_path = os.path.join(folder_path, f"block_{block_number}.json")
    save_block_data_to_json(block_data, file_path)
    fetched_block_numbers.append(block_number)


def get_latest_block_number():

    response = requests.get(f"{API_URL}?module=proxy&action=eth_blockNumber&apikey={API_KEY}")
    if response.status_code == 200:
        data = response.json()
        return int(data.get("result", "0"), 16)
    else:
        print("Failed to fetch data from the API.")
        return 0


def get_block_timestamp(block_number):  

    response = requests.get(f"{API_URL}?module=proxy&action=eth_getBlockByNumber&tag={block_number}&boolean=true&apikey={API_KEY}")
    if response.status_code == 200:        
        data = response.json()       
        return int(data.get("result", {}).get("timestamp", 0), 16)
    else:
        print(f"Failed to fetch block {block_number} data from the API.")
        return 0


def get_block_transactions(block_number):    

    response = requests.get(f"{API_URL}?module=proxy&action=eth_getBlockByNumber&tag={block_number}&boolean=true&apikey={API_KEY}")
    if response.status_code == 200:
        data = response.json()      
        transactions = data.get("result", {}).get("transactions", [])       
        return transactions
    else:
        print(f"Failed to fetch block {block_number} data from the API.")
        return []
    
    
def save_block_data_to_json(block_data, filename):   

    with open(filename, 'w') as json_file:
                json.dump(block_data, json_file, indent=4)

    print(f"Block data saved to JSON file: {filename}")


def load_block_numbers(filename):

    try:
        with open(filename, 'r') as file:
            block_numbers = json.load(file)
    except FileNotFoundError:
        block_numbers = []
    
    return block_numbers


def main(block_numbers_or_num_blocks, progress_callback=None, check_interrupt=None):       
    latest_block_number = get_latest_block_number()
    print(latest_block_number)
    fetched_block_numbers = load_block_numbers(BLOCKS_DATA_FILE)
    folder_path = os.path.join(current_directory, 'blocks_data')
    start_time = time.time()
  
    result_queue = Manager().Queue()

    num_processes = 4
    pool = Pool(processes=num_processes)

    if not os.path.exists(folder_path):
            os.makedirs(folder_path)

    total_processed_blocks = Manager().Value('i', 0)
    interrupt_flag = Manager().Value('b', False)
    def error_callback(e):
        print(f"Error occurred: {e}")

    def check_interrupt_wrapper():       
        if check_interrupt:
            interrupt_flag.value = check_interrupt()
        return interrupt_flag.value
      
    def update_progress_callback_blocks_download(x):
        nonlocal total_processed_blocks
        with progress_lock:            
                total_processed_blocks.value += 1 if x is None else x
                progress_value = total_processed_blocks.value

                if progress_callback:
                    if isinstance(block_numbers_or_num_blocks, list):
                        progress_callback(len(block_numbers_or_num_blocks), progress_value)     
                    else:
                        progress_callback(block_numbers_or_num_blocks, progress_value)     

                try:
                    block_number = result_queue.get_nowait()                    
                    if block_number is not None:
                        fetched_block_numbers.append(block_number)
                except Empty:
                    print("Result queue is empty, no block number to process.")

                if total_processed_blocks.value % 50 == 0:
                    save_block_data_to_json(fetched_block_numbers, BLOCKS_DATA_FILE)

    if isinstance(block_numbers_or_num_blocks, list):        
        target_block_numbers = block_numbers_or_num_blocks

    elif isinstance(block_numbers_or_num_blocks, int):            
        target_block_numbers = list(range(latest_block_number - 1, latest_block_number - block_numbers_or_num_blocks - 1, -1))      

    else:
        raise ValueError("Invalid input to main(): must be either a list of block numbers or an integer number of blocks")

    for block_number in target_block_numbers:
        if check_interrupt_wrapper():
            print("Interrupt flag is set. Stopping...")
            interrupt_flag.value = True 
            break
        print("Adding block to process:", block_number)
        pool.apply_async(process_block, args=(block_number, result_queue, fetched_block_numbers, interrupt_flag),
        callback=update_progress_callback_blocks_download
        , error_callback=error_callback)
        time.sleep(0.5)
            
    pool.close()
    pool.join()
    time.sleep(1)

    if not check_interrupt():       
        save_block_data_to_json(fetched_block_numbers, BLOCKS_DATA_FILE)
        
        missing_blocks = [block_number for block_number in target_block_numbers if block_number not in fetched_block_numbers]
        if missing_blocks:
            print(f"Missing blocks detected: {missing_blocks}")
            for block_number in missing_blocks:
                download_single_block(block_number, fetched_block_numbers)
                time.sleep(REQUEST_DELAY)
        
    end_time = time.time()
    total_execution_time = end_time - start_time
    print(f"Całkowity czas wykonania: {total_execution_time} sekundy")