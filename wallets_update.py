import datetime
import os
import json

current_directory = os.path.dirname(__file__)

def update_top_wallets_info(top_wallets_info, new_data, data):
    for wallet_data in new_data:
        address = wallet_data["wallet adress"]
        balance = wallet_data["wallet balance"]
        transaction_amount = wallet_data["biggest transaction amount in ether"]
        transaction_type = wallet_data["biggest transaction type(buy/sell)"]

        if address in top_wallets_info:
            existing_balance_entries = [entry for entry in top_wallets_info[address]["balance_history"] if entry["date"] == data["time"]]
            if not existing_balance_entries:
                top_wallets_info[address]["balance_history"].append({
                    "date": data["time"],
                    "balance": balance
                })

            if transaction_type == "buy":
                if top_wallets_info[address]["top_buy_transaction"] is None or transaction_amount > top_wallets_info[address]["top_buy_transaction"]["amount"]:
                    top_wallets_info[address]["top_buy_transaction"] = {
                        "amount": transaction_amount,
                        "date": data["time"]
                    }
            elif transaction_type == "sell":
                if top_wallets_info[address]["top_sell_transaction"] is None or transaction_amount < top_wallets_info[address]["top_sell_transaction"]["amount"]:
                    top_wallets_info[address]["top_sell_transaction"] = {
                        "amount": transaction_amount,
                        "date": data["time"]
                    }
 
        else:
            top_wallets_info[address] = {
                "balance_history": [{
                    "date": data["time"],
                    "balance": balance
                }],
 
                "top_buy_transaction": {
                    "amount": transaction_amount,
                    "date": data["time"]
                } if transaction_type == "buy" else None,
                "top_sell_transaction": {
                    "amount": transaction_amount,
                    "date": data["time"]
                } if transaction_type == "sell" else None
            }


def save_top_wallets_info(input_file_name, progress_callback=None, check_interrupt=None):
    input_file_path = os.path.join(current_directory, "interesting_info", input_file_name)  
    output_file_path = os.path.join(current_directory, "interesting_info", "Biggest_wallets_activity.json")  

    with open(input_file_path, 'r') as input_file:
        data = json.load(input_file)
        top_buyers = data["top 5 buyers"]
        top_sellers = data["top 5 sellers"]
        new_data = top_buyers + top_sellers

    with open(output_file_path, 'r') as output_file:
        top_wallets_info = json.load(output_file)

    update_top_wallets_info(top_wallets_info, new_data, data)

    with open(output_file_path, 'w') as output_file:
        json.dump(top_wallets_info, output_file, indent=4)

    print("zaaktualizowane dane portfeli!")