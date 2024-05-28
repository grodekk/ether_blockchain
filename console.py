import json
import os
import time
import datetime
import blocks_download
# from blocks_download import progress_callback
import blocks_extractor
import wallets_update
import files_checker
import database_tool
from datetime import datetime
from blocks_download import main, get_latest_block_number, get_block_timestamp, load_block_numbers, save_block_data_to_json


import blocks_remover
# import interface
import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QAction, QMenuBar, QVBoxLayout, QWidget, QPushButton
import sqlite3


import cProfile
# from PyQt5.QtWidgets import QApplication, QMainWindow, QAction, QMenuBar, QVBoxLayout, QWidget, QPushButton
# from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
# from matplotlib.figure import Figure
        


if __name__ == "__main__":

    # app = QApplication([])
    # main_window = EthereumDataApp()
    # main_window.show()
    # app.exec_() 
    current_directory = os.path.dirname(__file__)
    input_file_path = os.path.join(current_directory, "baza_danych.db")  
    # Nazwa podfolderu z danymi o blokach
    blocks_data_folder = "blocks_data"

# Utwórz pełną ścieżkę do katalogu "blocks_data"
    blocks_data_path = os.path.join(current_directory, blocks_data_folder)

    db_filename = input_file_path
    files_checker.check_files()

    # interface.qq()
    from datetime import datetime
















    # def get_timestamp_of_last_block_before_date(target_date):
    #     # Konwertuj docelową datę na timestamp
    #     target_timestamp = int(datetime.strptime(target_date, "%Y-%m-%d").timestamp())

    #     # Pobierz najnowszy numer bloku
    #     latest_block_number = get_latest_block_number()

    #     # Określ zakres początkowy i końcowy wyszukiwania
    #     start_block_number = latest_block_number
    #     end_block_number = 0

    #     # Rozpocznij przeszukiwanie
    #     while start_block_number >= end_block_number:
    #         # Sprawdź środkowy blok w zakresie
    #         mid_block_number = (start_block_number + end_block_number) // 2
    #         mid_block_timestamp = get_block_timestamp(hex(mid_block_number))

    #         # Wypisz numer środkowego bloku
    #         print(f"Checking block number: {mid_block_number}")

    #         # Jeśli środkowy blok ma datę późniejszą, zmniejsz zakres o połowę
    #         if mid_block_timestamp >= target_timestamp:
    #             start_block_number = mid_block_number - 1
    #         # Jeśli środkowy blok ma datę wcześniejszą, sprawdź poprzednie bloki w zakresie
    #         else:
    #             # Sprawdź datę poprzedniego bloku
    #             prev_block_timestamp = get_block_timestamp(hex(mid_block_number - 1))
    #             # Sprawdź czy poprzedni blok ma datę wcześniejszą
    #             if prev_block_timestamp < target_timestamp:
    #                 # Sprawdź czy godzina to 23:59
    #                 if datetime.utcfromtimestamp(prev_block_timestamp).strftime('%H:%M') == "23:59":
    #                     return prev_block_timestamp
    #             # W przeciwnym razie, zmniejsz zakres do lewej połowy
    #             end_block_number = mid_block_number + 1

    #     # Jeśli nie znaleziono bloku z poprzedzającą datą i godziną 23:59, zwróć None
    #     return None

    def get_timestamp_of_last_block_before_date(target_date):
        # Konwertuj docelową datę na timestamp
        # target_timestamp = int(datetime.strptime(target_date, "%Y-%m-%d").timestamp())
        target_timestamp = int(datetime.strptime(target_date + " 23:59", "%Y-%m-%d %H:%M").timestamp())


        # Pobierz najnowszy numer bloku
        latest_block_number = get_latest_block_number()

        # Określ zakres początkowy i końcowy wyszukiwania
        start_block_number = latest_block_number
        end_block_number = 0

        # Licznik iteracji (dodany w celu śledzenia liczby iteracji)
        iterations = 0

        # Rozpocznij przeszukiwanie
        while start_block_number >= end_block_number:
            iterations += 1
            # Sprawdź środkowy blok w zakresie
            mid_block_number = (start_block_number + end_block_number) // 2
            mid_block_timestamp = get_block_timestamp(hex(mid_block_number))

            # Wypisz numer środkowego bloku i jego timestamp
            print(f"Checking block number: {mid_block_number}, Timestamp: {mid_block_timestamp}")

            # Jeśli środkowy blok ma datę późniejszą, zmniejsz zakres o połowę
            if mid_block_timestamp >= target_timestamp:
                start_block_number = mid_block_number - 1
            # W przeciwnym razie, zwiększ zakres o połowę
            else:
                end_block_number = mid_block_number + 1

        # Wypisz liczbę iteracji (do debugowania)
        print("Total iterations:", iterations)

        # Jeśli nie znaleziono bloku z daną datą, zwróć None
        return start_block_number + 1





    print()
    print("1. Pobierz dane z bloków")
    print("2. Twórz raport godzinowy")
    print("3. Twórz raport dobowy")
    print("4. Zaaktualizuj dane największych portfeli")
    print("5. Usuń bloki")
    print("6. Eksportuj dane transakcji d/h do bazy danych")
    print("7. print 6(test)")
    print("8. Eksportuj dane największych portfeli do bazy danych")
    print("9. print 8(test) ")
    print("10. Tabela opłaty transakcyjne h") 
    print("11. Tabela opłaty transakcyjne d")   
    print("12. Tabela ilość transakcji h")
    print("13. Aktywność portfeli 100-1000 eth")                      
    choice = input("Wybierz tryb (1 lub 2 lub 3): ")
    print()


    if choice == "1":

        num_blocks = 10      
        blocks_download.main(num_blocks)


    elif choice == "2":


        extract_date = "2024-05-27 00:00:00"
            # Utwórz obiekt profilera
        profiler = cProfile.Profile()
        
        # Uruchom funkcję z profilowaniem
        profiler.run("blocks_extractor.extract_hourly_data(extract_date)")
        
        # Wygeneruj raport profilowania
        profiler.print_stats(sort='cumulative')

        
        # blocks_extractor.extract_hourly_data(extract_date)


    elif choice == "3":

        extract_date = "2024-02-21 00:00:00"
        blocks_extractor.extract_daily_data(extract_date)
    

    elif choice == "4":

        input_file_name = "2024-05-02_daily_data.json"                     
        wallets_update.save_top_wallets_info(input_file_name)


    elif choice == "5":

        input_folder = blocks_data_path
        delete_start_time = datetime.datetime(2024, 5, 15, 0, 0, 0)  
        delete_end_time = datetime.datetime(2024, 5, 30, 23, 0, 0) 
        
        blocks_remover.remove_blocks_in_time_range(input_folder, delete_start_time, delete_end_time)
        

    elif choice == "6":

        
        input_file_name = "2024-05-05_daily_data.json"
        # input_file_name = "2023-09-06_hourly_data.json"
        data_type = "daily"
        database_tool.import_data_to_combined_table(input_file_name, db_filename, data_type)
        

    elif choice == "7":

        
        data_type = "daily"
        database_tool.print_combined_data_by_type(db_filename, data_type)


    elif choice == "8":

        
        input_file_name = "Biggest_wallets_activity.json"
        database_tool.save_biggest_wallets_activity_database(input_file_name, db_filename)   
        

    elif choice == "9":

        
        database_tool.read_and_display_data_from_database(db_filename)      


    elif choice == "10":
        target_date = "2024-05-26"
        get_timestamp_of_last_block_before_date(target_date)

    

    # elif choice == "10":
    #     charts_tool.hourly_transaction_fee_chart(db_filename)

    # elif choice == "11":

        
    #     charts_tool.daily_transaction_fee_chart(db_filename)
    
    # elif choice == "12":

        
    #     charts_tool.hourly_transaction_number_chart(db_filename)

    # elif choice == "13":

        
    #     whales_activity(db_filename)


    # else:
    #     print("Błędny wybór. Wybierz 1 lub 2.")


    