import json
import os
import time
import datetime
import blocks_download
from datetime import datetime, timezone
import blocks_extractor
import wallets_update
import files_checker
import database_tool
from datetime import datetime
import asyncio
import blocks_remover
import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QAction, QMenuBar, QVBoxLayout, QWidget, QPushButton
import sqlite3
import cProfile
from datetime import datetime

if __name__ == "__main__":
    current_directory = os.path.dirname(__file__)
    input_file_path = os.path.join(current_directory, "baza_danych.db") 
    blocks_data_folder = "blocks_data"
    blocks_data_path = os.path.join(current_directory, blocks_data_folder)
    db_filename = input_file_path
    files_checker.check_files()

    print()
    print("1. Pobierz dane z bloków")
    print("2. Twórz raport godzinowy")
    print("3. Twórz raport dobowy")
    print("4. Zaaktualizuj dane największych portfeli")
    print("5. Usuń bloki")
    print("6. Eksportuj dane transakcji d/h do bazy danych")
    print("7. Wyświetl wpisy wszystkich sald portfeli w bazie danych")
    print("8. Eksportuj dane największych portfeli do bazy danych")
    print("9. Wyświetl wpisy największych portfeli w bazie danych ")
    print("10. Usuń puste wpisy z bazy danych") 
    print("11. Uruchom automatyzacje programu")   
    print("")
    print("")                      
    choice = input("Wybierz tryb: ")
    print()

    if choice == "1":
        num_blocks = blocks_download.BlockInput.get_num_blocks_to_fetch()
        
        if num_blocks is not None:
            print(num_blocks)            
            main_app = blocks_download.MainBlockProcessor(blocks_download.Config())
            main_app.run(num_blocks)

    elif choice == "2":
        extract_date = "2024-08-08 00:00:00"               
        blocks_extractor.extract_hourly_data(extract_date)            

    elif choice == "3":
        extract_date = "2024-08-08 00:00:00"
        blocks_extractor.extract_daily_data(extract_date)
    
    elif choice == "4":
        input_file_name = "2024-07-27_daily_data.json"                     
        wallets_update.save_top_wallets_info(input_file_name)

    elif choice == "5":
        input_folder = blocks_data_path
        delete_start_time = datetime.datetime(2024, 5, 15, 0, 0, 0)  
        delete_end_time = datetime.datetime(2024, 5, 30, 23, 0, 0)         
        blocks_remover.remove_blocks_in_time_range(input_folder, delete_start_time, delete_end_time)
        
    elif choice == "6":        
        input_file_name = "2024-07-09_daily_data.json"        
        data_type = "daily"
        database_tool.import_data_to_combined_table(input_file_name, db_filename, data_type)

        input_file_name = "2024-07-09_hourly_data.json"        
        data_type = "hourly"
        database_tool.import_data_to_combined_table(input_file_name, db_filename, data_type)
        
    elif choice == "7":        
        data_type = "daily"
        database_tool.print_combined_data_by_type(db_filename, data_type)

    elif choice == "8":        
        input_file_name = "Biggest_wallets_activity.json"
        database_tool.save_biggest_wallets_activity_database(input_file_name, db_filename)   
        1
    elif choice == "9":        
        database_tool.read_and_display_data_from_database(db_filename)      

    elif choice == "10":       
        database_tool.remove_invalid_entries(db_filename)

    elif choice == "11":
        start_date = "2024-07-01"        
        update_interval = 0.1  # Interwał w minutach
        automator = automation.BlockAutomator(start_date, update_interval)
        automator.run()            

    else:
        print("Błędny wybór.")   