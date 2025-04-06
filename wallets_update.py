import os
import json
from config import Config
from error_handler import ErrorHandler
from logger import logger


@ErrorHandler.ehdc()
class WalletInfoUpdater:
    def update_top_wallets_info(self, top_wallets_info, new_data, timestamp):
        for wallet_data in new_data:
            address = wallet_data["wallet address"]
            balance = wallet_data["wallet balance"]
            transaction_amount = wallet_data["biggest transaction amount in ether"]
            transaction_type = wallet_data["biggest transaction type (buy/sell)"]

            if address in top_wallets_info:
                self._update_existing_wallet(
                    top_wallets_info[address], balance, transaction_amount, transaction_type, timestamp)
            else:
                self._add_new_wallet(top_wallets_info,
                                     address,
                                     balance,
                                     transaction_amount,
                                     transaction_type,
                                     timestamp
                                     )

                logger.info(f"New wallet added with balance {balance} ETH "
                            f"and biggest {transaction_type} transaction {transaction_amount} ETH"
                            )

    def _update_existing_wallet(self, wallet_info, balance, transaction_amount, transaction_type, timestamp):
        if not any(entry["date"] == timestamp for entry in wallet_info["balance_history"]):
            wallet_info["balance_history"].append({"date": timestamp, "balance": balance})

        if transaction_type == "buy":
            if (wallet_info["top_buy_transaction"] is None or
                transaction_amount > wallet_info["top_buy_transaction"]["amount"]):
                wallet_info["top_buy_transaction"] = {
                    "amount": transaction_amount, "date": timestamp
                }

            logger.info(f"New top buy transaction recorded for existing wallet: {transaction_amount} ETH")

        elif transaction_type == "sell":
            if (wallet_info["top_sell_transaction"] is None or
                transaction_amount < wallet_info["top_sell_transaction"]["amount"]):
                wallet_info["top_sell_transaction"] = {
                    "amount": transaction_amount, "date": timestamp
                }

            logger.info(f"New top sell transaction recorded for existing wallet: {transaction_amount} ETH")


    def _add_new_wallet(self, top_wallets_info, address, balance, transaction_amount, transaction_type, timestamp):
        top_buy_transaction = {
            "amount": transaction_amount,
            "date": timestamp
        } if transaction_type == "buy" else None

        top_sell_transaction = {
            "amount": transaction_amount,
            "date": timestamp
        } if transaction_type == "sell" else None

        top_wallets_info[address] = {
            "balance_history": [{"date": timestamp, "balance": balance}],
            "top_buy_transaction": top_buy_transaction,
            "top_sell_transaction": top_sell_transaction
        }

        logger.info(f"New wallet added with initial balance {balance}")


@ErrorHandler.ehdc()
class WalletInfoManager:
    def __init__(self, config, updater):
        self.config = config        
        self.updater = updater

    def save_top_wallets_info(self, input_file_name, progress_callback=None, check_interrupt=None):
        INPUT_FILE_PATH = os.path.join(self.config.BASE_DIR, "interesting_info", input_file_name)

        new_data, timestamp = self._load_new_data(INPUT_FILE_PATH)
        top_wallets_info = self._load_existing_data(self.config.OUTPUT_FILE_PATH)

        self.updater.update_top_wallets_info(top_wallets_info, new_data, timestamp)
        self._save_data(self.config.OUTPUT_FILE_PATH, top_wallets_info)

        logger.info('Biggest wallets data updated!')

    def _load_new_data(self, input_file_path):
        with open(input_file_path, 'r') as input_file:
            data = json.load(input_file)
            new_data = data["top 5 buyers"] + data["top 5 sellers"]
            timestamp = data["time"]
        return new_data, timestamp

    def _load_existing_data(self, output_file_path):
        with open(output_file_path, 'r') as output_file:
            return json.load(output_file)

    def _save_data(self, output_file_path, data):
        with open(output_file_path, 'w') as output_file:
            json.dump(data, output_file, indent=4)


@ErrorHandler.ehdc()
class WalletUpdaterFactory:
    @staticmethod
    def create_wallets_updater():
        config = Config()
        updater = WalletInfoUpdater()
        manager = WalletInfoManager(config, updater)
        return manager


if __name__ == "__main__":
    """
    Mainly for testing    
    """
    input_file_name = "2024-10-02_daily_data.json"
    manager = WalletUpdaterFactory.create_wallets_updater()
    manager.save_top_wallets_info(input_file_name)