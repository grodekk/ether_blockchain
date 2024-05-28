import os
import sqlite3
import json


current_directory = os.path.dirname(__file__)

db_filename = os.path.join(current_directory, 'baza_danych.db')
wallets_activity_filename = os.path.join(current_directory, 'interesting_info', 'Biggest_wallets_activity.json')


def check_files():

    if not os.path.exists(db_filename):
            conn = sqlite3.connect(db_filename)
            cursor = conn.cursor()

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS combined_data (
                    id INTEGER PRIMARY KEY,
                    data_type TEXT,
                    date DATE,
                    hour INTEGER,
                    transactions_number INTEGER,
                    average_transaction_fee REAL,
                    wallet_0_1_eth INTEGER,
                    wallet_1_10_eth INTEGER,
                    wallet_10_100_eth INTEGER,
                    wallet_100_1000_eth INTEGER,
                    wallet_0_1_to_1_eth INTEGER,
                    wallet_above_10000_eth INTEGER,
                    wallet_1000_10000_eth INTEGER
                )
            ''')

        
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS wallet_balance (
                    id INTEGER PRIMARY KEY,
                    wallet_address TEXT,
                    date DATE,
                    balance REAL,
                    last_update_date DATE,
                    top_buy_amount REAL,
                    top_buy_date DATE,
                    top_sell_amount REAL,
                    top_sell_date DATE,
                    UNIQUE(wallet_address, date)
                )
            ''')

        
            conn.commit()
            conn.close()
            print(f'Baza danych "{db_filename}" i tabela zostały utworzone.')


    if not os.path.exists(wallets_activity_filename):
            with open(wallets_activity_filename, 'w') as json_file:
                empty_data = {}
                json.dump(empty_data, json_file)
            print(f'Plik "{wallets_activity_filename}" został utworzony z pustym słownikiem.')

    return True

  