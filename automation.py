import time
import os
import schedule
import json
from datetime import datetime, timezone, timedelta
from blocks_download import main, get_latest_block_number, get_block_timestamp, load_block_numbers, save_block_data_to_json, get_timestamp_of_first_block_on_target_date, get_timestamp_of_last_block_on_target_date
import blocks_extractor
import wallets_update
import database_tool
import blocks_remover

current_directory = os.path.dirname(__file__)
input_file_path = os.path.join(current_directory, "baza_danych.db")  
db_filename = input_file_path
BLOCKS_DATA_PATH = os.path.join(os.path.dirname(__file__), "blocks_data.json")
PROGRESS_DATA_PATH = os.path.join(os.path.dirname(__file__), "progress.json")


class BlockAutomator:
    def __init__(self, start_date, update_interval,progress_callback=None,check_interrupt=None):
        self.start_date = start_date
        self.update_interval = update_interval
        self.fetched_block_numbers = load_block_numbers(BLOCKS_DATA_PATH)
        self.progress_callback = progress_callback        
        self.check_interrupt = check_interrupt or (lambda: None)
        self.is_task_running = False     
        self.jobs = []
        print("BlockAutomator initialized with start_date:", start_date, "and update_interval:", update_interval)
        

    def load_progress(self):
        if os.path.exists(PROGRESS_DATA_PATH):
            with open(PROGRESS_DATA_PATH, "r") as f:
                print("Loading existing progress file.")
                return json.load(f)
        else:
            print("No progress file found, starting fresh.")
            return {}


    def save_progress(self, progress):
        with open(PROGRESS_DATA_PATH, "w") as f:
            print("Saving progress to file.")
            json.dump(progress, f, indent=4)


    def fetch_blocks_since_start_date(self):
        if self.is_task_running:
            print("Zadanie już jest w toku, nie dodajemy nowego.")
            return 

        
        self.is_task_running = True
        print("Rozpoczęcie przetwarzania bloków od daty startowej.")
        try:
            current_date = datetime.utcnow().date()
            target_date = datetime.strptime(self.start_date, "%Y-%m-%d").date()
            progress = self.load_progress()

        
            while target_date <= current_date:
                print(f'sprawdzanie interrupt{print(self.check_interrupt)}')

                if self.check_interrupt():
                    print("funkcja fetch blocks - Automatyzacja przerwana przez użytkownika.")
                    break 

                print(f"Processing blocks for date: {target_date}")
                self.process_day(str(target_date), progress)
                target_date = target_date + timedelta(days=1)
                if not self.check_interrupt():
                    self.save_progress(progress)
                print(f"Finished processing date: {target_date - timedelta(days=1)}")

        finally:            
            self.is_task_running = False
            print("Zakończenie przetwarzania bloków.")


    def process_day(self, target_date, progress):
        if self.check_interrupt():
            print("Proces dnia przerwany.")
            return

        if target_date not in progress:
            first_block = get_timestamp_of_first_block_on_target_date(target_date)
            last_block = get_timestamp_of_last_block_on_target_date(target_date)
            progress[target_date] = {
                "first_block": first_block,
                "last_block": last_block,
                "blocks_fetched": False,
                "reports_generated": False,
                "balances_updated": False,
                "data_exported": False,
                "data_cleaned": False
            }
            self.save_progress(progress)
        first_block = progress[target_date]["first_block"]
        last_block = progress[target_date]["last_block"]
        blocks_fetched = progress[target_date]["blocks_fetched"]
          
        if self.is_today(target_date):
            last_block = get_latest_block_number()
            progress[target_date]["last_block"] = last_block

        if not blocks_fetched:
            print('NOT BLOCK FETCHED')
            blocks_to_fetch = list(range(first_block, last_block + 1))
            fetched_blocks = [block for block in blocks_to_fetch if block not in self.fetched_block_numbers]
            if fetched_blocks:                
                print(len(fetched_blocks))
                main(fetched_blocks,progress_callback=self.progress_callback,check_interrupt=self.check_interrupt)
                if self.check_interrupt():
                    print("Pobieranie bloków przerwane.")
                    return  
                    
                self.fetched_block_numbers.extend(fetched_blocks)
                save_block_data_to_json(self.fetched_block_numbers, BLOCKS_DATA_PATH)
            if not self.is_today(target_date): 
                print('POBRANO BLOKI')                
                progress[target_date]["blocks_fetched"] = True
            if not self.check_interrupt():                
                self.save_progress(progress) 

        if not self.check_interrupt():
            if progress[target_date]["blocks_fetched"] and not self.is_today(target_date):
                print('ZAPISYWANIE WYNIKÓW!')
                self.process_reports(target_date, progress)
                self.update_balances(target_date, progress)
                self.export_data(target_date, progress)
                self.clean_data(target_date, progress)


    def process_reports(self, target_date, progress):
        if not progress[target_date]["reports_generated"]:
            self.generate_hourly_report(target_date)
            self.generate_daily_report(target_date)
            progress[target_date]["reports_generated"] = True
            self.save_progress(progress)


    def update_balances(self, target_date, progress):
        if not progress[target_date]["balances_updated"]:
            self.update_wallet_balances(target_date)
            progress[target_date]["balances_updated"] = True
            self.save_progress(progress)


    def export_data(self, target_date, progress):
        if not progress[target_date]["data_exported"]:
            self.export_to_database(target_date)
            progress[target_date]["data_exported"] = True
            self.save_progress(progress)


    def clean_data(self, target_date, progress):
        if not progress[target_date]["data_cleaned"]:
            self.clean_blocks_data(target_date, progress)
            progress[target_date]["data_cleaned"] = True
            self.save_progress(progress)


    def is_today(self, target_date):
        return target_date == datetime.utcnow().date().strftime("%Y-%m-%d")


    def schedule_regular_updates(self):
        self.clear_scheduled_tasks() 
        schedule.every(self.update_interval).minutes.do(self.fetch_blocks_since_start_date)


    def schedule_regular_updates(self):
        print("Scheduling regular updates.")
        self.clear_scheduled_tasks()  
        job = schedule.every(self.update_interval).minutes.do(self.fetch_blocks_since_start_date)
        self.jobs.append(job)
        print("Scheduled job:", job)


    def clear_scheduled_tasks(self):
        print("Clearing scheduled tasks.")
        for job in self.jobs:
            schedule.cancel_job(job)
        self.jobs = []
            

    def run(self):   
        self.schedule_regular_updates()
        while True:
            print(f"Checking interrupt in run method: {self.check_interrupt}")
            if self.check_interrupt and self.check_interrupt():
                print("Automatyzacja przerwana przez użytkownika.")
                self.clear_scheduled_tasks() 
                break 
            schedule.run_pending()
            self.print_scheduled_jobs() 
            time.sleep(1)  


    def generate_hourly_report(self, target_date):
        date_time_str = f"{target_date} 00:00:00"
        blocks_extractor.extract_hourly_data(date_time_str)


    def generate_daily_report(self, target_date):
        date_time_str = f"{target_date} 00:00:00"
        blocks_extractor.extract_daily_data(date_time_str)
        

    def update_wallet_balances(self, target_date):        
        input_file_name = f"{target_date}_daily_data.json"
        wallets_update.save_top_wallets_info(input_file_name)


    def export_to_database(self, target_date):                
        input_file_name = f"{target_date}_daily_data.json"
        data_type = "daily"
        database_tool.import_data_to_combined_table(input_file_name, db_filename, data_type)
        data_type = "hourly"
        input_file_name = f"{target_date}_hourly_data.json"
        database_tool.import_data_to_combined_table(input_file_name, db_filename, data_type)      
        input_file_name = "Biggest_wallets_activity.json"  
        database_tool.save_biggest_wallets_activity_database(input_file_name, db_filename)   


    def clean_blocks_data(self, target_date, progress):
        first_block = progress[target_date]["first_block"]
        last_block = progress[target_date]["last_block"]
        blocks_remover.remove_blocks_in_range(first_block, last_block)
        print("Czyszczenie zakończone")


    def print_scheduled_jobs(self):
        jobs = schedule.get_jobs()
        print(f"Number of scheduled jobs: {len(jobs)}")
        for job in jobs:
            print(f"Job: {job} - Interval: {job.interval} {job.unit} - Last run: {job.last_run}")


if __name__ == "__main__":
    start_date = "2024-07-01"        
    update_interval = 0.1  # Interwał w minutach
    automator = BlockAutomator(start_date, update_interval)
    automator.run()