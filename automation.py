import time
import os
import schedule
from datetime import datetime
from blocks_download import main, get_latest_block_number, get_block_timestamp, load_block_numbers, save_block_data_to_json

BLOCKS_DATA_PATH = os.path.join(os.path.dirname(__file__), "blocks_data.json")

class BlockAutomator:
    def __init__(self, start_date, update_interval):
        self.start_date = start_date
        self.update_interval = update_interval
        self.fetched_block_numbers = load_block_numbers(BLOCKS_DATA_PATH)
      
    def fetch_blocks_since_start_date(self):
        start_timestamp = int(datetime.strptime(self.start_date, "%Y-%m-%d").timestamp())
        latest_block_number = get_latest_block_number()        
   

        
        for block_number in range(latest_block_number, 0, -1):
                print(f"Processing block number: {block_number}")

                try:
                        if block_number in self.fetched_block_numbers:
                            print(f"Block {block_number} already fetched. Stopping early.")
                            break


                        timestamp = get_block_timestamp(hex(block_number))
                        if timestamp < start_timestamp:
                            print(f"Reached start timestamp. Stopping fetching blocks.")
                            break

                                            
                        num_blocks = latest_block_number - max(self.fetched_block_numbers, default=0)
                        print(f'NUM_BLOCKS -> {num_blocks}')
                        main(num_blocks)
                        self.fetched_block_numbers = load_block_numbers(BLOCKS_DATA_PATH)
                        break

                except Exception as e:
                        print(f"Error fetching block {block_number}: {e}")

                    # Po zakończeniu pętli, zapisz stan bloków do pliku
        # save_block_data_to_json(self.fetched_block_numbers, BLOCKS_DATA_PATH)
        

                
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
    start_date = "2024-05-24"
    update_interval = 2  # Interwał w minutach
    automator = BlockAutomator(start_date, update_interval)
    automator.run()
