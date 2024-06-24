import time
import os
import schedule
from datetime import datetime, timezone
from blocks_download import main, get_latest_block_number, get_block_timestamp, load_block_numbers, save_block_data_to_json,  get_timestamp_of_first_block_on_target_date

BLOCKS_DATA_PATH = os.path.join(os.path.dirname(__file__), "blocks_data.json")



class BlockAutomator:
    def __init__(self, start_date, update_interval):
        self.start_date = start_date
        self.update_interval = update_interval
        self.fetched_block_numbers = load_block_numbers(BLOCKS_DATA_PATH)

    def fetch_blocks_since_start_date(self):
        start_timestamp = int(datetime.strptime(self.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
        latest_block_number = get_latest_block_number()       

        # Użyj binarnego wyszukiwania do znalezienia bloku tuż przed start_date
        starting_block_number =  get_timestamp_of_first_block_on_target_date(self.start_date)
        print(f"Starting block number for date {self.start_date}: {starting_block_number}") 

        # Zbierz listę wszystkich bloków do pobrania między starting_block_number a latest_block_number
        target_block_numbers = list(range(latest_block_number, starting_block_number, -1))

        # Filtruj bloki, które już zostały pobrane
        blocks_to_fetch = [block_number for block_number in target_block_numbers if block_number not in self.fetched_block_numbers]
        # print(f"Blocks to fetch: {blocks_to_fetch}")

        # Przekaż listę bloków do funkcji main()
        if blocks_to_fetch:
            print(f'liczba bloków do pobrania :{len(blocks_to_fetch)}')
            main(blocks_to_fetch)
   

    def schedule_regular_updates(self):
        # schedule.every(self.update_interval).minutes.do(main)
        schedule.every(self.update_interval).minutes.do(self.fetch_blocks_since_start_date)
        

    def run(self):
        # self.fetch_blocks_since_start_date()
        self.schedule_regular_updates()

        while True:
            schedule.run_pending()
            time.sleep(1)

# Przykład użycia:
if __name__ == "__main__":
    start_date = "2024-06-24"    
    update_interval = 0.1  # Interwał w minutach
    automator = BlockAutomator(start_date, update_interval)
    automator.run()
