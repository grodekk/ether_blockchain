import time
import os
import schedule
import json
from datetime import datetime, timezone, timedelta
from blocks_download import MainBlockProcessor, FileManager, BlockTimestampFinder, EtherAPI
from config import Config
import blocks_extractor
import wallets_update
import database_tool
import blocks_remover
from logger import logger


class TaskScheduler:
    def __init__(self, block_processor, update_interval, check_interrupt=None):
        self.block_processor = block_processor
        self.update_interval = update_interval
        self.check_interrupt = check_interrupt or (lambda: False)
        self.is_running = False


    def run(self):                   
        self.schedule_regular_updates()
        while True:            
            if self.check_interrupt():
                logger.info("automation process stopped by user")
                self.clear_scheduled_tasks() 
                break 

            schedule.run_pending()          
            self.print_scheduled_jobs()  
            time.sleep(1)  


    def schedule_regular_updates(self):
        print("Scheduling regular updates.")
        self.clear_scheduled_tasks()                  
        schedule.every(self.update_interval).minutes.do(self.run_task)


    def run_task(self):       
        if self.is_running:
            logger.debug("Task is running")
            return

        self.is_running = True
        
        try:
            self.block_processor.run_sequential_processing()
        finally:
            logger.debug("Task ended")
            self.is_running = False
            

    def clear_scheduled_tasks(self):
        logger.debug("Clearing scheduling tasks")
        schedule.clear()


    def print_scheduled_jobs(self):
        jobs = schedule.get_jobs()
        logger.debug(f"number of scheduled jobs: {len(jobs)}")
        for job in jobs:
            logger.debug(f"Job: {job} - Time interval: {job.interval} {job.unit} - Last time activated: {job.last_run}")



class BlockProcessor:
    def __init__(self, config, progress_manager, block_timestamp_finder, 
                block_fetcher, data_processor, start_date, progress_callback=None, check_interrupt=None):
    
        self.config = config
        self.progress_manager = progress_manager
        self.block_timestamp_finder = block_timestamp_finder        
        self.block_fetcher = block_fetcher
        self.data_processor = data_processor        
        self.start_date = start_date                
        self.progress_callback = progress_callback or (lambda *args, **kwargs: None)
        self.check_interrupt = check_interrupt or (lambda: False)           
        
        print("BlockAutomator initialized with start_date:", start_date)

        
    def run_sequential_processing(self):        
                
        for date in self.iterate_dates():
            target_date = str(date)
            self.initialize_progress_for_date(target_date, self.progress_manager_progress)

            if target_date in self.progress_manager.progress:                
                self.process_remaining_tasks(target_date, self.progress_manager.progress)              
                self.process_unfetched_blocks(target_date, self.progress_manager.progress)      


    def process_unfetched_blocks(self, target_date, progress):
        if not progress[target_date].get("blocks_fetched", False):
            logger.info(f"[{target_date}] Pobranie bloków rozpoczęte...")

            first_block, last_block = self.progress_manager.get_block_range_for_date(target_date)
                 
            self.block_fetcher.fetch_and_save_new_blocks(first_block, last_block, target_date, progress)      


    def process_remaining_tasks(self, target_date, progress):
        if progress[target_date].get("blocks_fetched", False) and not all(progress[target_date].values()):
            logger.info(f"[{target_date}] Procesowanie pozostałych zadań...")

            if not self.check_interrupt():                        
                self.finalize_day_processing(target_date, progress)


    def is_today(self, target_date):
        return target_date == datetime.utcnow().date().strftime("%Y-%m-%d")


    def iterate_dates(self):        
        current_date = datetime.utcnow().date()
        target_date = datetime.strptime(self.start_date, "%Y-%m-%d").date()
        while target_date <= current_date:
            yield target_date
            target_date += timedelta(days=1)

   #todo
    def initialize_progress_for_date(self, target_date, progress):   
        if target_date not in progress:
            print(f"Inicjalizowanie postępu dla daty: {target_date}")
            first_block = self.block_timestamp_finder.get_timestamp_of_first_block_on_target_date(target_date)
            last_block = self.block_timestamp_finder.get_timestamp_of_last_block_on_target_date(target_date)
            self.progress_manager.create_date_progress(target_date, first_block, last_block)


    def finalize_day_processing(self, target_date, progress):        
        if progress[target_date]["blocks_fetched"] and not self.is_today(target_date):
            print('ZAPISYWANIE WYNIKÓW!')
            self.data_processor.process_reports(target_date, progress)
            self.data_processor.update_balances(target_date, progress)
            self.data_processor.export_data(target_date, progress)
            self.data_processor.clean_data(target_date, progress)

class BlockFetcher:

    def __init__(self, config, file_manager, main_block_processor, progress_manager, progress_callback=None, check_interrupt=None):               
        self.config = config
        self.file_manager = file_manager
        self.main_block_processor = main_block_processor
        self.progress_manager = progress_manager             
        self.fetched_block_numbers = self.file_manager.load_from_json(self.config.BLOCKS_DATA_FILE)
        self.progress_callback = progress_callback or  (lambda *args, **kwargs: None)
        self.check_interrupt = check_interrupt or (lambda: False)

            
    def fetch_and_save_new_blocks(self, first_block, last_block, target_date, progress):
            new_blocks = self.fetch_blocks(first_block, last_block)
            if new_blocks:
                self.fetched_block_numbers = self.file_manager.load_from_json(self.config.BLOCKS_DATA_FILE)  
                unique_new_blocks = self.get_unique_new_blocks(new_blocks)
                save_block_data_to_json(unique_new_blocks, self.config.BLOCKS_DATA_FILE)
                if not self.is_today(target_date):                     
                    self.progress_manager.update_task_progress(target_date, task_name="blocks_fetched")


    def fetch_blocks(self, first_block, last_block):
            print(f"Pobieranie bloków od {first_block} do {last_block}.")
            blocks_to_fetch = list(range(first_block, last_block + 1))
            new_blocks = [block for block in blocks_to_fetch if block not in self.fetched_block_numbers]

            if new_blocks:
                print(f"Liczba nowych bloków: {len(new_blocks)}.")                
                print(self.progress_callback, self.check_interrupt)
                self.main_block_processor.run(new_blocks, progress_callback=self.progress_callback, check_interrupt=self.check_interrupt)

                if check_interrupt():
                    print("Pobieranie bloków przerwane przez użytkownika.")
                    return []

                return new_blocks


    def get_unique_new_blocks(self, new_blocks):
        fetched_blocks_set = set(self.fetched_block_numbers)
        new_blocks_set = set(new_blocks)
        return list(new_blocks_set - fetched_blocks_set)


class ProgressManager:
    def __init__(self, config, check_interrupt=None):
        self.config = config
        self.progress = progress_manager.load_progress()
        self.check_interrupt = check_interrupt or (lambda: False)  

    def update_task_progress(self, target_date, task_name):
        self.progress[target_date][task_name] = True
        self.progress_manager.save_progress(self.progress)

        
    def is_task_complete(self, target_date, task_name):
        """Sprawdza, czy zadanie dla danej daty zostało ukończone."""
        return self.progress.get(target_date, {}).get(task_name, False)       
        
    
    def create_date_progress(self, target_date, first_block, last_block):    
        self.progress[target_date] = {
            "first_block": first_block,
            "last_block": last_block,
            "blocks_fetched": False,
            "reports_generated": False,
            "balances_updated": False,
            "data_exported": False,
            "data_cleaned": False,
        }
        self.save_progress(self.progress)

    def load_progress(self):
        if os.path.exists(self.config.PROGRESS_DATA_FILE):
            with open(self.config.PROGRESS_DATA_FILE, "r") as f:
                print("Loading existing progress file.")
                return json.load(f)
        else:
            print("No progress file found, starting fresh.")
            return {}
    

    def save_progress(self):
        if not self.check_interrupt(): 
            with open(self.config.PROGRESS_DATA_FILE, "w") as f:
                print("Saving self.progress to file.")
                json.dump(self.progress, f, indent=4)


    def get_block_range_for_date(self, target_date):        
        
        first_block = self.progress[target_date]["first_block"]
        last_block = self.progress[target_date]["last_block"]        

        if self.is_today(target_date):
            last_block = get_latest_block_number()
            self.progress[target_date]["last_block"] = last_block
        
        return first_block, last_block


class DataProcessor:


    def process_reports(self, target_date, progress):
        if not self.progress[target_date]["reports_generated"]:
            self.generate_hourly_report(target_date)
            self.generate_daily_report(target_date)
            self.progress[target_date]["reports_generated"] = True
            self.save_progress(self.progress)


    def update_balances(self, target_date, progress):
        if not self.progress[target_date]["balances_updated"]:
            self.update_wallet_balances(target_date)
            self.progress[target_date]["balances_updated"] = True
            self.save_progress(self.progress)


    def export_data(self, target_date, progress):
        if not self.progress[target_date]["data_exported"]:
            self.export_to_database(target_date)
            self.progress[target_date]["data_exported"] = True
            self.save_progress(self.progress)


    def clean_data(self, target_date, progress):
        if not self.progress[target_date]["data_cleaned"]:
            self.clean_blocks_data(target_date, self.progress)
            self.progress[target_date]["data_cleaned"] = True
            self.save_progress(self.progress)

    
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
        first_block = self.progress[target_date]["first_block"]
        last_block = self.progress[target_date]["last_block"]
        blocks_remover.remove_blocks_in_range(first_block, last_block)
        print("Czyszczenie zakończone")


class AutomationFactory:
    @staticmethod
    def create_automator(config, start_date, update_interval=0.01, progress_callback=None, check_interrupt=None):        
        ether_api = EtherAPI(config)
        file_manager = FileManager(config)
        main_block_processor = MainBlockProcessor(config)      

        progress_manager = ProgressManager(config) 
        # shared_progress = progress_manager.load_progress()
        
        block_fetcher = BlockFetcher(
            config=config,
            file_manager=file_manager,
            main_block_processor=main_block_processor,
            progress_manager=progress_manager,
            progress_callback=progress_callback,
            check_interrupt=check_interrupt,
        )
                
        
        block_timestamp_finder = BlockTimestampFinder(ether_api)
        data_processor = DataProcessor(shared_progress)
        
        block_processor = BlockProcessor(
            shared_progress=shared_progress
            config=config,
            progress_manager=progress_manager,
            block_timestamp_finder=block_timestamp_finder,
            block_fetcher=block_fetcher,
            data_processor=data_processor,
            progress = shared_progress
            start_date=start_date,
            progress_callback=progress_callback,
            check_interrupt=check_interrupt,
        )
        
        task_scheduler = TaskScheduler(
            block_processor=block_processor,
            update_interval=update_interval,
            check_interrupt=check_interrupt,
        )

        return task_scheduler


if __name__ == "__main__":       
    config = Config()
    automator = AutomationFactory.create_automator(
        config=config,
        start_date="2023-12-30",
        progress_callback=lambda total, current: print(f"progress: {current}/{total}"),
        check_interrupt=False,
    )

    automator.run()