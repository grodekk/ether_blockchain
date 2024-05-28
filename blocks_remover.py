import os
from datetime import datetime
import json
import time

def remove_blocks_in_time_range(input_folder, delete_start_time, delete_end_time, progress_callback=None):

    start_time = time.time()

    json_files = [file for file in os.listdir(input_folder) if file.endswith(".json")]

    for json_file in json_files:
        with open(os.path.join(input_folder, json_file), 'r+') as file:
            block_data = json.load(file)
            block_timestamp = int(block_data["timestamp"])
            block_datetime_utc = datetime.utcfromtimestamp(block_timestamp)
                        
            if delete_start_time <= block_datetime_utc <= delete_end_time:
                file.close()
                os.remove(os.path.join(input_folder, json_file))
                print(f"Removed: {json_file}")

    end_time = time.time()
    total_execution_time = end_time - start_time
    print(f"Całkowity czas wykonania: {total_execution_time} sekundy")