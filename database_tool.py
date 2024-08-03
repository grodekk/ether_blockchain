import sqlite3
import json
from datetime import datetime
import os


current_directory = os.path.dirname(__file__)


def save_biggest_wallets_activity_database(input_file_name, db_filename):
    input_file_path = os.path.join(current_directory, "interesting_info", input_file_name)  

    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()
    
    with open(input_file_path, "r") as file:
        wallet_data = json.load(file)

    for wallet_address, wallet_info in wallet_data.items():
        if isinstance(wallet_info, dict):
            balance_history = wallet_info.get('balance_history', [])

            top_buy_transaction = wallet_info.get('top_buy_transaction', None)
            top_sell_transaction = wallet_info.get('top_sell_transaction', None)

            top_buy_amount = top_buy_transaction.get('amount', None) if top_buy_transaction else None
            top_buy_date_str = top_buy_transaction.get('date', None) if top_buy_transaction else None
            top_buy_date = datetime.strptime(top_buy_date_str, "%Y-%m-%d").date() if top_buy_date_str else None

            top_sell_amount = top_sell_transaction.get('amount', None) if top_sell_transaction else None
            top_sell_date_str = top_sell_transaction.get('date', None) if top_sell_transaction else None
            top_sell_date = datetime.strptime(top_sell_date_str, "%Y-%m-%d").date() if top_sell_date_str else None
            
            for entry in balance_history:
                date_str = entry['date']
                balance = entry['balance']
                datetime_obj = datetime.strptime(date_str, "%Y-%m-%d")
                date = datetime_obj.date()

                last_update_date_str = datetime.now().strftime("%Y-%m-%d")
                last_update_date = datetime.strptime(last_update_date_str, "%Y-%m-%d").date()

                cursor.execute('SELECT * FROM wallet_balance WHERE wallet_address = ? AND date = ? AND balance = ?',
                (wallet_address, date, balance))
                
                existing_entry = cursor.fetchone()

                if existing_entry:
                    print(f"Entry for wallet {wallet_address}, date {date}, and balance {balance} already exists.")
                else:
                    cursor.execute('''
                        INSERT OR REPLACE INTO wallet_balance (
                            wallet_address, date, balance,
                            last_update_date, top_buy_amount, top_buy_date,
                            top_sell_amount, top_sell_date
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        wallet_address, date, balance,
                        last_update_date, top_buy_amount, top_buy_date,
                        top_sell_amount, top_sell_date
                    ))
    
    conn.commit()
    conn.close()


def table_data_calculations(entry, cursor, data_type):
        date = entry.get('time', None)           
        transactions_number = entry.get('transactions number', None)        
        average_transaction_fee = entry.get('average transaction fee', None)
        wallet_classification = entry.get('wallet classification in eth balance', {})

        wallet_keys = ['0.1 ETH', '1-10 ETH', '10-100 ETH', '100-1000 ETH', '0.1-1 ETH', 'Above 10000 ETH', '1000-10000 ETH']
        wallet_values = [wallet_classification.get(key, 0) for key in wallet_keys]

        if data_type == 'daily':
            hour = None
        elif data_type == 'hourly':
            hour_str = entry.get('time', None)
            hour_dt = datetime.strptime(hour_str, "%Y-%m-%d %H:%M:%S")
            hour = hour_dt.hour

        if data_type == 'daily':
            cursor.execute('SELECT * FROM combined_data WHERE date = ?', (date,))
            existing_entry = cursor.fetchone()
        elif data_type == 'hourly':
            cursor.execute('SELECT * FROM combined_data WHERE date = ? AND hour = ?', (date, hour))
            existing_entry = cursor.fetchone()
        if existing_entry:            
            print(f"Entry for date {date} and hour {hour} already exists.")
        
        else:
            cursor.execute('''
                INSERT INTO combined_data (
                    data_type, date, hour, transactions_number, average_transaction_fee,
                    wallet_0_1_eth, wallet_1_10_eth, wallet_10_100_eth,
                    wallet_100_1000_eth, wallet_0_1_to_1_eth, wallet_above_10000_eth,
                    wallet_1000_10000_eth
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data_type, date, hour, transactions_number, average_transaction_fee,
                *wallet_values
            ))    
     

def import_data_to_combined_table(input_file_name, db_filename, data_type, progress_callback=None, check_interrupt=None):
    input_file_path = os.path.join(current_directory, "interesting_info", input_file_name)  
        
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()

    with open(input_file_path, "r") as file:
        data = json.load(file)

    if data_type == "hourly":
        for entry in data:
            table_data_calculations(entry, cursor, data_type)

    if data_type == "daily":        
        table_data_calculations(data, cursor, data_type)
               
    conn.commit()
    conn.close()


def print_combined_data_by_type(db_filename, data_type):    
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT * FROM combined_data WHERE data_type = ?
    ''', (data_type,))

    rows = cursor.fetchall()

    conn.close()

    for row in rows:
        print("Data Type:", row[1])
        print("Date:", row[2])
        print("Hour:", row[3])
        print("Transactions Number:", row[4])
        print("Average Transaction Fee:", row[5])
        print("Wallet Classification:")
        print("  0.1 ETH:", row[6])
        print("  1-10 ETH:", row[7])
        print("  10-100 ETH:", row[8])
        print("  100-1000 ETH:", row[9])
        print("  0.1-1 ETH:", row[10])
        print("  Above 10000 ETH:", row[11])
        print("  1000-10000 ETH:", row[12])
        print("\n")


def read_and_display_data_from_database(db_filename):
    conn = sqlite3.connect(db_filename)
    conn.row_factory = sqlite3.Row  
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM wallet_balance
    ''')

    rows = cursor.fetchall()

    for row in rows:
        wallet_address = row['wallet_address']
        date = row['date']
        balance = row['balance']
        last_update_date = row['last_update_date']
        top_buy_amount = row['top_buy_amount']
        top_buy_date = row['top_buy_date']
        top_sell_amount = row['top_sell_amount']
        top_sell_date = row['top_sell_date']

        print("Wallet Address:", wallet_address)
        print("Date:", date)
        print("Balance:", balance)
        print("Last Update Date:", last_update_date)
        print("Top Buy Amount:", top_buy_amount)
        print("Top Buy Date:", top_buy_date)
        print("Top Sell Amount:", top_sell_amount)
        print("Top Sell Date:", top_sell_date)
        print("\n")    

    conn.commit()
    conn.close()

def check_date_in_database(selected_date, chart_name, sql_query_check, DATABASE_PATH):
    connection = None
    try:
        connection = sqlite3.connect(DATABASE_PATH)
        cursor = connection.cursor()
      
        cursor.execute(sql_query_check, (selected_date.strftime("%Y-%m-%d"),))
        result = cursor.fetchone()
        
        
        if result:           
            return True
            
        else:            
            return False
            
    except Exception as e:       
        print("Błąd sprawdzania daty w bazie danych:", str(e))
        return False

    finally:       
        if connection:
            connection.close()


def remove_invalid_entries(db_filename):
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()    
    
    cursor.execute('''
        DELETE FROM combined_data
        WHERE (transactions_number IS NULL OR transactions_number = 0)
        AND (average_transaction_fee IS NULL OR average_transaction_fee = 0)
        AND (wallet_0_1_eth = 0)
        AND (wallet_1_10_eth = 0)
        AND (wallet_10_100_eth = 0)
        AND (wallet_100_1000_eth = 0)
        AND (wallet_0_1_to_1_eth = 0)
        AND (wallet_above_10000_eth = 0)
        AND (wallet_1000_10000_eth = 0)
    ''')
    
    conn.commit()
    conn.close()