import os
from datetime import datetime
import json
import time

current_directory = os.path.dirname(__file__)

def remove_blocks_in_time_range(input_folder, delete_start_time, delete_end_time, progress_callback=None, check_interrupt=None):
    json_files = [file for file in os.listdir(input_folder) if file.endswith(".json")]

    for json_file in json_files:
        print('asd')
        with open(os.path.join(input_folder, json_file), 'r+') as file:
            block_data = json.load(file)
            block_timestamp = int(block_data["timestamp"])
            block_datetime_utc = datetime.utcfromtimestamp(block_timestamp)
                        
            if delete_start_time <= block_datetime_utc <= delete_end_time:
                file.close()
                os.remove(os.path.join(input_folder, json_file))
                print(f"Removed: {json_file}")
                

def remove_blocks_in_range(first_block, last_block):
        input_folder =  os.path.join(current_directory, "blocks_data")  
        start_time = time.time()
        for block_num in range(first_block, last_block + 1):
            print(f'deleted - {block_num}')
            block_file_name = f"block_{block_num}.json"
            block_file_path = os.path.join(input_folder, block_file_name)
            
            if os.path.exists(block_file_path):
                os.remove(block_file_path)
                print(f"Usunięto: {block_file_name}")
            else:
                print(f"Plik nie istnieje: {block_file_name}")
        end_time = time.time()
        total_execution_time = end_time - start_time