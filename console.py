import blocks_download
import blocks_extractor
import wallets_update
import files_checker
import database_tool
import blocks_remover
import config
import automation
from datetime import datetime, timezone


class ConsoleApp:
    def __init__(self):
        self.config = config.Config()
        self.menu_actions = {
            "1": self.download_block_data,
            "2": self.create_hourly_report,
            "3": self.create_daily_report,
            "4": self.update_top_wallets,
            "5": self.remove_blocks,
            "6": self.export_transaction_data,
            "7": self.display_daily_combined_data,
            "8": self.export_top_wallets,
            "9": self.display_wallets_balances,
            "10": self.clean_database,
            "11": self.run_automation,
            "12": self.find_first_block,
            "13": self.find_last_block,
            "q": self.quit_program
        }


    @staticmethod
    def display_menu():
        print("\n=== Blockchain Data Processing Tool ===")
        print("1. Download block data")
        print("2. Create hourly report")
        print("3. Create daily report")
        print("4. Update top wallet data")
        print("5. Remove blocks")
        print("6. Export transaction data to database")
        print("7. Display all combined data")
        print("8. Export top wallets to database")
        print("9. Display wallets balances in database")
        print("10. Remove empty database entries")
        print("11. Run program automation")
        print("12. Find first block of date")
        print("13. Find last block of date")
        print("h. Help")
        print("q. Quit")
        print("========================================\n")


    @staticmethod
    def get_user_choice():
        return input("Enter your choice: ")

    def download_block_data(self):
        block_input = blocks_download.BlockInput()
        num_blocks = block_input.get_num_blocks_to_fetch()
        main_block_processor = blocks_download.MainBlockProcessor(self.config)
        main_block_processor.run(
            num_blocks,
            progress_callback=lambda total, current: print(f"Progress: {current}/{total}"),
            check_interrupt=lambda: False
        )



    @staticmethod
    def create_hourly_report():
        try:
            user_input = input("Enter date for hourly report (YYYY-MM-DD): ").strip()
            datetime.strptime(user_input, "%Y-%m-%d")

            extract_date = f"{user_input} 00:00:00"
            print(extract_date)
            hourly_extractor = blocks_extractor.ExtractorFactory.create_extractor('hourly')
            hourly_extractor.extract_data(extract_date)

        except ValueError:
            print("Invalid date format! Please enter in YYYY-MM-DD format.")


    @staticmethod
    def create_daily_report():
        try:
            user_input = input("Enter date for daily report (YYYY-MM-DD): ").strip()
            datetime.strptime(user_input, "%Y-%m-%d")

            extract_date = f"{user_input} 00:00:00"
            daily_extractor = blocks_extractor.ExtractorFactory.create_extractor('daily')
            daily_extractor.extract_data(extract_date)

        except ValueError:
            print("Invalid date format! Please enter in YYYY-MM-DD format.")


    @staticmethod
    def update_top_wallets():
        try:
            date_input = input("Enter date (YYYY-MM-DD): ")
            datetime.strptime(date_input, "%Y-%m-%d")
            file_name = f"{date_input}_daily_data.json"

            manager = wallets_update.WalletUpdaterFactory.create_wallets_updater()
            manager.save_top_wallets_info(file_name)

        except ValueError:
            print("Invalid date format! Please enter in YYYY-MM-DD format.")


    def remove_blocks(self):
        try:
            start_date = input("Enter start date (YYYY-MM-DD): ")
            end_date = input("Enter end date (YYYY-MM-DD): ")

            delete_start_time = datetime.strptime(
                f"{start_date} 00:00:00",
                "%Y-%m-%d %H:%M:%S"
            ).replace(tzinfo=timezone.utc)

            delete_end_time = datetime.strptime(
                f"{end_date} 23:59:59",
                "%Y-%m-%d %H:%M:%S"
            ).replace(tzinfo=timezone.utc)

            blocks_remover_instance = blocks_remover.BlocksRemover(
                self.config,
                blocks_download.FileManager(self.config)
            )

            blocks_remover_instance.remove_blocks_in_time_range(delete_start_time, delete_end_time)

        except ValueError:
            print(f"Invalid date format! Please enter in YYYY-MM-DD format.")


    def export_transaction_data(self):
        try:
            date_input = input("Enter date (YYYY-MM-DD): ")
            datetime.strptime(date_input, "%Y-%m-%d")

            daily_file = f"{date_input}_daily_data.json"
            hourly_file = f"{date_input}_hourly_data.json"

            db_components = database_tool.DatabaseFactory.create_database_components(self.config)

            db_components["data_importer"].import_data_to_combined_table(
                daily_file,
                "daily"
            )

            db_components["data_importer"].import_data_to_combined_table(
                hourly_file,
                "hourly"
            )

        except ValueError:
            print("Invalid date format! Please enter in YYYY-MM-DD format.")


    def display_daily_combined_data(self):
        try:
            date_input = input("Enter date (YYYY-MM-DD): ")
            datetime.strptime(date_input, "%Y-%m-%d")

            db_components = database_tool.DatabaseFactory.create_database_components(self.config)
            db_components["data_display"].print_combined_data_by_type("daily")

        except ValueError:
            print("Invalid date format! Please enter in YYYY-MM-DD format.")


    def display_hourly_combined_data(self):
        try:
            date_input = input("Enter date (YYYY-MM-DD): ")
            datetime.strptime(date_input, "%Y-%m-%d")

            db_components = database_tool.DatabaseFactory.create_database_components(self.config)
            db_components["data_display"].print_combined_data_by_type("hourly")

        except ValueError:
            print("Invalid date format! Please enter in YYYY-MM-DD format.")


    def export_top_wallets(self):
        input_file_name = "Biggest_wallets_activity.json"
        db_components = database_tool.DatabaseFactory.create_database_components(self.config)
        db_components["save_biggest_wallets"].save_biggest_wallets_activity_database(input_file_name)


    def display_wallets_balances(self):
        db_components = database_tool.DatabaseFactory.create_database_components(self.config)
        db_components["data_reader"].read_and_display_data_from_database()


    def clean_database(self):
        db_components = database_tool.DatabaseFactory.create_database_components(self.config)
        db_components["data_cleaner"].remove_invalid_entries()


    def run_automation(self):
        try:
            start_date = input("Enter start date (YYYY-MM-DD): ")
            datetime.strptime(start_date, "%Y-%m-%d")

            automator = automation.AutomationFactory.create_automator(
                config=self.config,
                start_date=start_date,
                progress_callback=lambda total, current: print(f"progress: {current}/{total}"),
                check_interrupt=False,
            )

            automator.run()

        except ValueError:
            print("Invalid date format! Please enter in YYYY-MM-DD format.")


    def find_first_block(self):
        api = blocks_download.EtherAPI(config=self.config)
        block_timestamp_finder = blocks_download.BlockTimestampFinder(ether_api=api)

        try:
            target_date = input("Enter target date (YYYY-MM-DD): ")
            first_block_number = block_timestamp_finder.get_timestamp_of_first_block_on_target_date(target_date)
            print(f"The first block on {target_date} is: {first_block_number}")

        except Exception as e:
            print(f"An error occurred: {e}")


    def find_last_block(self):
        api = blocks_download.EtherAPI(config=self.config)
        block_timestamp_finder = blocks_download.BlockTimestampFinder(ether_api=api)

        try:
            target_date = input("Enter target date (YYYY-MM-DD): ")
            last_block_number = block_timestamp_finder.get_timestamp_of_last_block_on_target_date(target_date)
            print(f"The last block on {target_date} is: {last_block_number}")

        except Exception as e:
            print(f"An error occurred: {e}")


    @staticmethod
    def quit_program():
        print("Exiting program...")
        exit(0)


    def run(self):
        checker = files_checker.FilesCheckerFactory.create_files_checker(self.config)
        checker.check_files()
        while True:
            self.display_menu()
            choice = self.get_user_choice()
            action = self.menu_actions.get(choice)
            if action:
                action()
            else:
                print("Invalid choice. Please try again.")

            input("\nPress Enter to continue...")


if __name__ == "__main__":
    app = ConsoleApp()
    app.run()