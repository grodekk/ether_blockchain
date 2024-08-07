from multiprocessing import cpu_count, Manager, Pool, Lock
from queue import Empty
import time
import requests 
import json
import os
from PyQt5.QtWidgets import QInputDialog
from datetime import datetime, timezone, timedelta


class Config:
    CURRENT_DIRECTORY = os.path.dirname(__file__)
    API_KEY = "ZA1WF2Z9ZFJBGWPI8N4C6F6ARUVD3K7K5E"
    API_URL = "https://api.etherscan.io/api"
    REQUEST_DELAY = 0.2
    BLOCKS_DATA_DIR = os.path.join(CURRENT_DIRECTORY, 'blocks_data')
    BLOCKS_DATA_FILE = os.path.join(CURRENT_DIRECTORY, 'blocks_data.json')


class BlockInput:
    @staticmethod
    def get_num_blocks_to_fetch(method="console"):
        if method == "console":
            try:
                return int(input("Wprowadź ilość bloków do pobrania: ")), True
            except ValueError:
                print("Zła wartość, wpisz liczbę całkowitą!")
                return None, False
                
        elif method == "interface":
            num_blocks, ok_pressed = QInputDialog.getInt(None, "Ilość bloków", "Wprowadź ilość bloków do pobrania:")
            if ok_pressed:
                return num_blocks, True
            else:
                return None, False
                
        else:
            raise ValueError("Zła metoda, użyj konsoli lub interfejsu")


class EtherAPI:
    def __init__(self, config):
        self.config = config

    def get_latest_block_number(self):
        response = requests.get(f"{self.config.API_URL}?module=proxy&action=eth_blockNumber&apikey={self.config.API_KEY}")
        if response.status_code == 200:
            data = response.json()
            return int(data.get("result", "0"), 16)
        else:
            print("Failed to fetch data from the API.")
            return 0

    def get_block_timestamp(self, block_number):  
        response = requests.get(f"{self.config.API_URL}?module=proxy&action=eth_getBlockByNumber&tag={block_number}&boolean=true&apikey={self.config.API_KEY}")
        if response.status_code == 200:        
            data = response.json()       
            return int(data.get("result", {}).get("timestamp", 0), 16)
        else:
            print(f"Failed to fetch block {block_number} data from the API.")
            return 0

    def get_block_transactions(self, block_number):    
        response = requests.get(f"{self.config.API_URL}?module=proxy&action=eth_getBlockByNumber&tag={block_number}&boolean=true&apikey={self.config.API_KEY}")
        if response.status_code == 200:
            data = response.json()      
            transactions = data.get("result", {}).get("transactions", [])       
            return transactions
        else:
            print(f"Failed to fetch block {block_number} data from the API.")
            return []


class BlocksManager:
    def __init__(self, api, config):
        self.api = api
        self.config = config      

    def process_block(self, block_number, result_queue, fetched_block_numbers, interrupt_flag=None):
        print(f'process block start flag = {interrupt_flag}')
        try:     
            if interrupt_flag.value:
                print(f"Przerwanie wykryte w bloku: {block_number}. Zakończono.")
                return None, 0
            
            print("Processing block:", block_number)
            
            if block_number in fetched_block_numbers:
                print(f"Block {block_number} already fetched. Skipping...")
                return None, 1        

            timestamp = self.api.get_block_timestamp(hex(block_number))
            if timestamp == 0:
                return None, 0

            transactions = self.api.get_block_transactions(hex(block_number))
            block_data = {
                "block_number": block_number,
                "timestamp": timestamp,
                "transactions": transactions
            }

            file_path = os.path.join(self.config.BLOCKS_DATA_DIR, f"block_{block_number}.json")       
            self.save_block_data_to_json(block_data, file_path)

            time.sleep(self.config.REQUEST_DELAY)            

            return block_number, 1      

        except Exception as e:
            print(f"Exception occurred in process_block for block {block_number}: {str(e)}")
            return None

    def get_timestamp_of_first_block_on_target_date(self, target_date):    
        target_timestamp = int(datetime.strptime(target_date + " 00:00", "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc).timestamp())    
        latest_block_number = self.api.get_latest_block_number()    
        start_block_number = latest_block_number
        end_block_number = 0
        
        while start_block_number > end_block_number:            

            mid_block_number = (start_block_number + end_block_number) // 2
            mid_block_timestamp = self.api.get_block_timestamp(hex(mid_block_number))        
            print(f"Checking block number: {mid_block_number}, Timestamp: {mid_block_timestamp}")
            
            if mid_block_timestamp > target_timestamp:
                start_block_number = mid_block_number - 1        
            else:
                end_block_number = mid_block_number + 1
        
        final_block_timestamp = self.api.get_block_timestamp(hex(start_block_number))
        if final_block_timestamp < target_timestamp:
            start_block_number += 1
        
        return start_block_number

    def get_timestamp_of_last_block_on_target_date(self, target_date):   
        next_day = datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=1)
        target_timestamp = int(next_day.replace(tzinfo=timezone.utc).timestamp())    
        latest_block_number = self.api.get_latest_block_number()        

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

    def download_single_block(self, block_number, fetched_block_numbers):
        print("Downloading single block:", block_number)
        if block_number in fetched_block_numbers:
            print(f"Block {block_number} already fetched. Skipping...")
            return

        timestamp = self.api.get_block_timestamp(hex(block_number))
        if timestamp == 0:
            return

        transactions = self.api.get_block_transactions(hex(block_number))
        block_data = {
            "block_number": block_number,
            "timestamp": timestamp,
            "transactions": transactions
        }

        file_path = os.path.join(self.config.BLOCKS_DATA_DIR, f"block_{block_number}.json")
        self.save_block_data_to_json(block_data, file_path)
        fetched_block_numbers.append(block_number)        
        
    def save_block_data_to_json(self, block_data, filename):
        with open(filename, 'w') as json_file:
                    json.dump(block_data, json_file, indent=4)

        print(f"Block data saved to JSON file: {filename}")

    def load_block_numbers(self, filename):
        try:
            with open(filename, 'r') as file:
                block_numbers = json.load(file)
        except FileNotFoundError:
            block_numbers = []
        
        return block_numbers


class MultiProcessor:
    def __init__(self):
        num_cores = cpu_count()
        self.num_processes = int(num_cores * 0.75)
        self.pool = Pool(processes=self.num_processes)
        self.manager = Manager()
        self.result_queue = self.manager.Queue()
        self.interrupt_flag = self.manager.Value('b', False)
        self.total_processed_blocks = self.manager.Value('i', 0)
        self.progress_lock = Lock()

    def apply_async(self, func, args, callback=None, error_callback=None):        
        self.pool.apply_async(func, args=args, callback=callback, error_callback=error_callback)        

    def update_progress(self, x, progress_callback, total_target, fetched_block_numbers, save_callback):        
        with self.progress_lock:           
            block_number, progress_increment = x
          
            if block_number is not None:
                self.result_queue.put(block_number)
                fetched_block_numbers.append(block_number)
        
            self.total_processed_blocks.value += progress_increment if progress_increment is not None else 0
            progress_value = self.total_processed_blocks.value
       
            if progress_callback:
                progress_callback(total_target, progress_value)
    
            if self.total_processed_blocks.value % 50 == 0:
                save_callback(fetched_block_numbers)

    def start(self, target_block_numbers, process_func, progress_callback, check_interrupt, fetched_block_numbers, save_callback):           
        def error_callback(e):
            print(f"Error occurred: {e}")

        def check_interrupt_wrapper():            
            if check_interrupt:
                self.interrupt_flag.value = check_interrupt()
            return self.interrupt_flag.value

        for block_number in target_block_numbers:
            if check_interrupt_wrapper():
                print("Interrupt flag is set. Stopping...")
                break
            print("Adding block to process:", block_number)

            self.apply_async(
                process_func,
                args=(block_number, self.result_queue, fetched_block_numbers, self.interrupt_flag),
                callback=lambda x: self.update_progress(x, progress_callback, len(target_block_numbers), fetched_block_numbers, save_callback),
                error_callback=error_callback
            )
            time.sleep(0.5)

        self.pool.close()
        self.pool.join()


class MainBlockProcessor:
    def __init__(self, config):
        self.config = config
        self.api = EtherAPI(config)
        self.blocks_manager = BlocksManager(self.api, config)               
        if not os.path.exists(self.config.BLOCKS_DATA_DIR):
            os.makedirs(self.config.BLOCKS_DATA_DIR)

    def get_target_block_numbers(self, block_numbers_or_num_blocks):
        latest_block_number = self.api.get_latest_block_number()

        if isinstance(block_numbers_or_num_blocks, list):
            return block_numbers_or_num_blocks
        elif isinstance(block_numbers_or_num_blocks, int):
            return list(
                range(
                    latest_block_number - 1,
                    latest_block_number - block_numbers_or_num_blocks - 1,
                    -1
                )
            )
        else:
            raise ValueError("Invalid input: must be either a list of block numbers or an integer number of blocks")

    def process_blocks(self, target_block_numbers, progress_callback=None, check_interrupt=None):
        processor = MultiProcessor()
        fetched_block_numbers = self.blocks_manager.load_block_numbers(self.config.BLOCKS_DATA_FILE)
        process_block = self.blocks_manager.process_block
        
        processor.start(
            target_block_numbers=target_block_numbers,
            process_func=process_block,
            progress_callback=progress_callback,
            check_interrupt=check_interrupt,
            fetched_block_numbers=fetched_block_numbers,
            save_callback=lambda fetched: self.blocks_manager.save_block_data_to_json(fetched, self.config.BLOCKS_DATA_FILE)
        )

        time.sleep(1)

        if check_interrupt and check_interrupt():
            return
        
        self.handle_missing_blocks(target_block_numbers, fetched_block_numbers)
        
        self.blocks_manager.save_block_data_to_json(fetched_block_numbers, self.config.BLOCKS_DATA_FILE)
        print('Fetched blocks data saved')

    def handle_missing_blocks(self, target_block_numbers, fetched_block_numbers):
        missing_blocks = [block_number for block_number in target_block_numbers if block_number not in fetched_block_numbers]
        if missing_blocks:
            print(f"Missing blocks detected: {missing_blocks}")
            for block_number in missing_blocks:
                self.blocks_manager.download_single_block(block_number, fetched_block_numbers)
                time.sleep(self.config.request_delay)

    def run(self, block_numbers_or_num_blocks, progress_callback=None, check_interrupt=None):
        target_block_numbers = self.get_target_block_numbers(block_numbers_or_num_blocks)
        self.process_blocks(target_block_numbers, progress_callback, check_interrupt)


if __name__ == "__main__":
    config = Config()
    block_processor = MainBlockProcessor(config)    
   
    block_numbers_or_num_blocks = 10 
   
    block_processor.run(
        block_numbers_or_num_blocks,
        progress_callback=lambda total, current: print(f"Postęp: {current}/{total}"),
        check_interrupt=lambda: False  
    )