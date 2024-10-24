import os
import json
import heapq
from datetime import datetime, timedelta
import time
from blocks_download import Config, EtherAPI, BlockProcessor, FileManager, BlockDownloader
from json.decoder import JSONDecodeError
import logging
from logger import LoggerConfig
from error_handler import CustomProcessingError
import files_checker
  
logger_config = LoggerConfig()
logger = logger_config.get_logger()


class BlockFileProcessor:
    """
    A class for proccesing files containing block data.
    
    Attributes
    ----------
    block_downloader : BlockDownloader
        An object used for single block download.
    file_manager : FileManager
        An object responsible for load/save files.

    Methods
    -------
    load_block_data
    """    
    def __init__(self, block_downloader, file_manager):
        self.block_downloader = block_downloader
        self.file_manager = file_manager    

    def load_block_data(self, json_file):   
        """
        Reads block data from JSON file. If the file is empty or corrupted, attempts to download missing
        block data and reloads it.

        Parameters
        ----------
        json_file : object
            The name of the JSON file containing data of one block.

        Returns
        -------
        dict
            A dictionary containing block data.

        Raises
        -------
        CustomProcessingError
        """     
        try:
            block_data = self.file_manager.load_from_json(json_file)            
            if not block_data:
                logger.warning(f"File {json_file} is empty or corrupted. Attempting to fetch missing data.")                                
                block_number = int(json_file.split('_')[1].split('.')[0])   
                self.block_downloader.download_single_block(block_number, [])
                block_data = self.file_manager.load_from_json(json_file)

            return block_data        
 
        except CustomProcessingError as cpe:
            raise

        except Exception as e:
            CustomProcessingError.handle_processing_exception(e, json_file=json_file)        


class TransactionsGrouper:
    """    
    A class for grouping blockchain transactions by the hour they occurred, using block data from JSON files.

    
    Attributes
    ----------
    block_file_processor: BlockFileProcessor
         An object used for processing files containing block data.
    transactions_by_hour: dictionary
        A dictionary to store transactions grouped by hour.
    config: Config
        Configuration object containing settings.

    Methods
    -------
    group_transactions_by_hour
    """
    def __init__(self, block_file_processor):
        self.block_file_processor = block_file_processor        
        self.transactions_by_hour = {}
        self.config = Config()
    
    def group_transactions_by_hour(self, progress_callback=None, check_interrupt=None):        
        """
        Groups transactions according to hour they occured.        
        Iterates through each JSON file listed in the configuration JSON FILES directory, loading block data and grouping transactions.        


        Parameters
        ----------
        progress_callback : callable, optional
            A callback function to report progress.
        check_interrupt : callable, optional
            A function to check if processing should be interrupted

        Returns
        -------
        dict            
            A dictionary where keys are hour strings and values are lists of transactions that occurred during that hour.

        Raises
        -------
        CustomProcessingError
        """     
        self.transactions_by_hour = {}
        total_files = len(self.config.JSON_FILES)
        processed_files = 0            

        logger.info(f"Total files to process: {total_files}")

        for json_file in self.config.JSON_FILES:
            try:            
                block_data = self.block_file_processor.load_block_data(json_file)

                if check_interrupt and check_interrupt():
                    logger.warning("Processing interrupted by user.")
                    break
                                
                block_timestamp = int(block_data["timestamp"])                    
                hour = datetime.utcfromtimestamp(block_timestamp).strftime("%Y-%m-%d %H:00:00")
                transactions = block_data['transactions']

                if hour not in self.transactions_by_hour:
                    self.transactions_by_hour[hour] = []
                self.transactions_by_hour[hour].extend(transactions)

                processed_files += 1
                progress_value = processed_files                
                if progress_callback:
                    progress_callback(total_files, progress_value) 


            except CustomProcessingError as cpe:                  
                raise

            except Exception as e:
                CustomProcessingError.handle_processing_exception(e, json_file=json_file)
         
        logger.info(f"Total files processed: {processed_files}/{total_files}")
        return self.transactions_by_hour


class TransactionProcessor:    
    """    
    A class for processing transactions.
    
    Attributes
    ----------
    total_transactions: int
        A number of all transactions for given day/hour.
    total_fees: float
        The total value of fees.   
    total_value_eth: float
        The total value of transactions in ETH.
        
    Methods
    -------
    group_transactions_by_hour
    """
    def __init__(self):
        self.total_transactions = 0
        self.total_fees = 0
        self.total_value_eth = 0

    def reset(self):        
        self.total_transactions = 0
        self.total_fees = 0.0        
        self.total_value_eth = 0.0
    
    def _summarize(self):
        summary = (f"Processing completed: Total transactions: {self.total_transactions},"
                   f"Total value: {self.total_value_eth} ETH, Total fees: {self.total_fees} ETH")

    def process_transaction(self, transaction):
        """       
        Retrieves detailed data for provided transaction including sender adress and receiver adress, value in eth, gas amount and gas price. 

        Parameters
        ----------
        transaction : dict
            A dictionary representing a single transaction from the blockchain, containing keys such as:
            'from', 'to', 'value', 'gas', and 'gasPrice'.

        Returns
        -------
        tuple
            A tuple containing the sender's address, receiver's address, and the transaction value in ETH.

        Raises
        -------
        CustomProcessingError
        """    
        try:
            sender = transaction["from"]            
            receiver = transaction["to"]
            value_wei = int(transaction["value"], 16)
            gas_price_wei = int(transaction["gasPrice"], 16)
            gas_wei = int(transaction["gas"], 16)

            value_eth = value_wei / 10**18
            transaction_fee_wei = gas_price_wei * gas_wei
            transaction_fee_eth = transaction_fee_wei / 10**18

            self.total_transactions += 1
            self.total_fees += transaction_fee_eth
            self.total_value_eth += value_eth                    

            return sender, receiver, value_eth

        except CustomProcessingError as cpe:
            raise

        except Exception as e:
            CustomProcessingError.handle_processing_exception(e)        


class WalletUpdater:
    """    
    A class for updating wallets.
    
    Attributes
    ----------
    wallets_transactions: dict
        A dictionary to store wallets transactions, where keys are wallet adresses and values are transaction values in eth.
        
    Methods
    -------
    reset
    update_wallets
    """
    def __init__(self):
        self.wallets_transactions = {}

    def reset(self):        
        # used to clear wallets transactions (for hourly data extract usage) #
        self.wallets_transactions = {}
        logger.debug("WalletUpdater.reset - Reset wallet transactions.")

    def update_wallets(self, sender, receiver, value_eth):
        """
        Updates the transaction history for the specified sender and receiver wallets.     
        Stores and updates wallets by appending each transaction value to a list of transactions 
        associated with the respective wallet address.

        Parameters
        ----------
        sender : str
            The wallet address initiating the transaction (selling).
        receiver : str
            The wallet address receiving the transaction (buying).
        value_eth : float
            The value of the transaction in ETH.
        
        Returns
        -------
        None            

        Raises
        -------
        CustomProcessingError
        """    
        try:            
            if sender not in self.wallets_transactions:
                self.wallets_transactions[sender] = []
            self.wallets_transactions[sender].append({"value": -value_eth, "type": "sell"})

            if receiver not in self.wallets_transactions:
                self.wallets_transactions[receiver] = []
            self.wallets_transactions[receiver].append({"value": value_eth, "type": "buy"})

        except CustomProcessingError as cpe:
            raise

        except Exception as e:
            CustomProcessingError.handle_processing_exception(e)        
            

class WalletClassifier:
    """    
    A class for classyfing wallets.
    
    Attributes
    ----------
    None
        
    Methods
    -------
    classify_wallet (static)
    classify_wallets
    """
    @staticmethod
    def classify_wallet(balance):
        """
        Assigns a classification to a wallet based on its balance.

        Parameters
        ----------
        balance : float
            Sum of transactions on a wallet.
        
        Returns
        -------
        str
            Classification of the wallet (e.g., "Above 10000 ETH").
        """    
        if balance >= 10000:
            return "Above 10000 ETH"
        elif balance >= 1000:
            return "1000-10000 ETH"
        elif balance >= 100:
            return "100-1000 ETH"
        elif balance >= 10:
            return "10-100 ETH"
        elif balance >= 1:
            return "1-10 ETH"
        elif balance >= 0.1:
            return "0.1-1 ETH"
        else:
            return "Below 0.1 ETH"


    def classify_wallets(self, wallets_transactions):
        """
        Classifies multiple wallets based on their total transaction values.              

        Parameters
        ----------
        wallets_transactions : dict
            A dictionary containing wallet transactions, where keys are wallet addresses
            and values are lists of transaction values in ETH. 
                    
        Returns
        -------
        wallets_balances : dict
            A dictionary that stores the number of wallets in each classification, where keys are classification labels
            (e.g., "100-1000 ETH") and values are the counts of wallets in those ranges.    

        Raises
        -------
        CustomProcessingError
        """            
        wallets_balances = {}
        try:
            for wallet, transactions in wallets_transactions.items():
                total_value_eth = sum(transaction["value"] for transaction in transactions)
                classification = self.classify_wallet(total_value_eth)
                if classification not in wallets_balances:
                    wallets_balances[classification] = 0
                wallets_balances[classification] += 1
        
            return wallets_balances        
        
        except CustomProcessingError as cpe:
            raise
        
        except Exception as e:
            CustomProcessingError.handle_processing_exception(e)      


class TopWalletsGenerator:
    """    
    A class for generating top buyers and sellers wallets.

    Methods
    -------
    get_top_wallets
    """
    def get_top_wallets(self, wallets_transactions, top_n=5, is_seller=False):
        """
        Generate addresses with the highest or lowest balances.      

        Parameters
        ----------
        wallets_transactions : dict
            A dictionary containing wallet transactions, where keys are wallet addresses
            and values are lists of transaction values in ETH. 
        top_n : int, optional
            The number of top wallets to generate. Defaults to 5.
        is_seller : bool
            If True, generate the top selling wallets. If False, generate the top buying wallets.
            Defaults to False.
                    
        Returns
        -------
        top_wallets_info : list of dict
            A list of dictionaries, each containing data about a top wallet.

        Raises
        -------
        CustomProcessingError
        """   
        try:
            key_func = (lambda x: -sum(t["value"] for t in x[1] if t["value"] < 0)) if is_seller else (lambda x: sum(t["value"] for t in x[1] if t["value"] > 0))
            top_wallets = heapq.nlargest(top_n, wallets_transactions.items(), key=key_func)

            top_wallets_info = []
            for wallet_info in top_wallets:
                wallet_address, transactions = wallet_info
                max_transaction_eth = min(transactions, key=lambda x: x["value"]) if is_seller else max(transactions, key=lambda x: x["value"])
                transaction_type = max_transaction_eth["type"]
                wallet_balance_eth = sum(transaction["value"] for transaction in transactions)
                wallet_info_with_balance = {
                    "wallet address": wallet_address,
                    "biggest transaction type (buy/sell)": transaction_type,
                    "biggest transaction amount in ether": max_transaction_eth["value"],
                    "wallet balance": wallet_balance_eth
                }
                top_wallets_info.append(wallet_info_with_balance)
            
            return top_wallets_info

        except CustomProcessingError as cpe:
            raise
        
        except Exception as e:
            CustomProcessingError.handle_processing_exception(e)      


class ResultFormatter:
    """
    This class formats collected results.   

    Methods
    -------
    format_result ( static )   
    """
    @staticmethod
    def format_result(start_hour_str, total_transactions, total_fees, wallets_balances, top_wallets_generator, wallets_transactions):
        """
        Formats collected overall data for provided day/hour.              

        Parameters
        ----------
        start_hour_str : str
            String parsed hour format representing each hour or day, where collecting results starts.
        total_transactions: int
            A number of all transactions for given day/hour.
        total_fees: float
            The total value of fees. 
        wallets_balances : dict
            A dictionary that stores the number of wallets in each classification, where keys are classification labels
            (e.g., "100-1000 ETH") and values are the counts of wallets in those ranges.     
        top_wallets_generator : object
            An object for generating top buyers and sellers wallets
        wallets_transactions : dict
            A dictionary containing wallet transactions, where keys are wallet addresses
            and values are lists of transaction values in ETH.  
                    
        Returns
        -------
        result_data : dict
            A dictionary which stores results from given hour/day.

        Raises
        -------
        CustomProcessingError
        """            
        try:
            average_fee_eth = total_fees / total_transactions if total_transactions > 0 else 0
            result_data = {
                "time": start_hour_str,
                "transactions number": total_transactions,
                "average transaction fee": average_fee_eth,
                "wallet classification in eth balance": wallets_balances,
                "top 5 buyers": top_wallets_generator.get_top_wallets(wallets_transactions, top_n=5, is_seller=False),
                "top 5 sellers": top_wallets_generator.get_top_wallets(wallets_transactions, top_n=5, is_seller=True)
            }

            return result_data

        except CustomProcessingError as cpe:
            raise
        
        except Exception as e:
            CustomProcessingError.handle_processing_exception(e, context="ResultFormatter.format_result")      


class DailyDataExtractor:
    """
    A central class for orchestrating the processing of daily blockchain blocks data extract procedure.     

    Parameters
    ----------
    transactions_grouper : TransactionsGrouper
    transaction_processor : TransactionProcessor
    wallet_updater : WalletUpdater
    wallet_classifier : WalletCrassifier
    top_wallets_generator : TopWalletsGenerator
    result_formatter : ResultFormatter

    Methods
    -------
    extract_data
    
    """    
    def __init__(self, transactions_grouper, transaction_processor, wallet_updater, wallet_classifier, top_wallets_generator, result_formatter):
        self.transactions_grouper = transactions_grouper
        self.transaction_processor = transaction_processor
        self.wallet_updater = wallet_updater
        self.wallet_classifier = wallet_classifier
        self.top_wallets_generator = top_wallets_generator
        self.result_formatter = result_formatter
        self.config = Config()

    def extract_data(self, extract_date, progress_callback=None, check_interrupt=None):
        """        
        Extracts daily blockchain transaction data and saves the result as a JSON file. 

        Parameters
        ----------
        extract_date : str
            The date in the format "%Y-%m-%d %H:%M:%S" for which data should be processed.
        progress_callback : callable, optional
            Callback to report progress.
        check_interrupt : callable, optional
            Function to check if the process should be interrupted.

        Returns
        -------
        None
        
        Raises
        -------
        CustomProcessingError        
        """    
        logger.info("Starting daily data extraction.")
        try:
            transactions_for_day = []

            start_hour = datetime.strptime(extract_date, "%Y-%m-%d %H:%M:%S")
            start_hour_str = start_hour.strftime("%Y-%m-%d")
            
            logger.debug("Starting transaction grouping for daily extraction.")    

            transactions_by_hour = self.transactions_grouper.group_transactions_by_hour(progress_callback, check_interrupt)

            logger.debug("Finished transaction grouping for daily extraction.")   
            
            for hour, transactions_in_hour in transactions_by_hour.items():
                hour_datetime = datetime.strptime(hour, "%Y-%m-%d %H:%M:%S")
                if hour_datetime.date() == start_hour.date():
                    transactions_for_day.extend(transactions_in_hour)   

            logger.debug("Starting update wallets for daily extraction.") 

            for transaction in transactions_for_day:
                sender, receiver, value_eth = self.transaction_processor.process_transaction(transaction)
                self.wallet_updater.update_wallets(sender, receiver, value_eth) 

            logger.debug("Finished update wallets for daily extraction.")     

            logger.debug("Starting classify wallets for daily extraction.")    

            wallets_balances = self.wallet_classifier.classify_wallets(self.wallet_updater.wallets_transactions)

            logger.debug("Finished classify wallets for daily extraction.")           

            logger.debug("Starting format results for daily extraction.")

            result_data = self.result_formatter.format_result(
                start_hour_str,
                self.transaction_processor.total_transactions,
                self.transaction_processor.total_fees,
                wallets_balances,
                self.top_wallets_generator,
                self.wallet_updater.wallets_transactions
            )                        
            logger.debug("Finished classify wallets for daily extraction.")    

            date_part = start_hour.strftime("%Y-%m-%d")
            output_file_path = os.path.join(self.config.CURRENT_DIRECTORY, self.config.OUTPUT_FOLDER, f"{date_part}_daily_data.json")
            with open(output_file_path, 'w') as output_file:
                json.dump(result_data, output_file, indent=4)

            logger.info(f"Daily data extraction completed for date {date_part}.")

        except CustomProcessingError as cpe:
            raise
        
        except Exception as e:
            CustomProcessingError.handle_processing_exception(e)      
        

class HourlyDataExtractor:
    """
    A central class for orchestrating the processing of hourly blockchain blocks data extract procedure.    

    Parameters
    ----------
    transactions_grouper : TransactionsGrouper
    transaction_processor : TransactionProcessor
    wallet_updater : WalletUpdater
    wallet_classifier : WalletCrassifier
    top_wallets_generator : TopWalletsGenerator
    result_formatter : ResultFormatter

    Methods
    -------
    extract_data
    """
    def __init__(self, transactions_grouper, transaction_processor, wallet_updater, wallet_classifier, top_wallets_generator, result_formatter):
        self.transactions_grouper = transactions_grouper
        self.transaction_processor = transaction_processor
        self.wallet_updater = wallet_updater
        self.wallet_classifier = wallet_classifier
        self.top_wallets_generator = top_wallets_generator
        self.result_formatter = result_formatter
        self.config = Config()

    def extract_data(self, extract_date, progress_callback=None, check_interrupt=None):
        """
        Extracts hourly blockchain transaction data and saves the result as a JSON file. 

        Parameters
        ----------
        extract_date : str
            Day to proceed.
        progress_callback : callable, optional
            A callback function to report progress.
        check_interrupt : callable, optional
            A function to check if processing should be interrupted.

        Returns
        -------
        None
        
        Raises
        -------
        CustomProcessingError        
        """   
        logger.info("Starting hourly data extraction.")
        try:
            hourly_results_all = []         

            start_hour = datetime.strptime(extract_date, "%Y-%m-%d %H:%M:%S")            
            current_hour = start_hour

            logger.debug("Starting transaction grouping for hourly extraction.")   

            transactions_by_hour = self.transactions_grouper.group_transactions_by_hour(progress_callback, check_interrupt)      

            logger.debug("Finished transaction grouping for hourly extraction.")   
            
            while current_hour.hour <= 23:
                end_hour_dt = current_hour + timedelta(hours=1)
                start_hour_str = current_hour.strftime("%Y-%m-%d %H:%M:%S")

                self.transaction_processor.reset()
                self.wallet_updater.reset()

                transactions_for_hour = transactions_by_hour.get(start_hour_str, [])
                
                logger.debug(f"Starting update wallets for {current_hour} hour.")   

                if transactions_for_hour:             
                    for transaction in transactions_for_hour:
                        sender, receiver, value_eth = self.transaction_processor.process_transaction(transaction)
                        self.wallet_updater.update_wallets(sender, receiver, value_eth)
                    logger.debug(f"Finished update wallets for {current_hour} hour.")    
                else:
                    logger.debug(f"No transactions for {current_hour} hour, skipping result formatting.")
                
                logger.debug(f"Starting classify wallets for {current_hour} hour.")  

                wallets_balances = self.wallet_classifier.classify_wallets(self.wallet_updater.wallets_transactions)

                logger.debug(f"Finished classify wallets for {current_hour} hour.")  
                
                logger.debug(f"Starting format results for {current_hour} hour.")

                result_data = self.result_formatter.format_result(
                    start_hour_str,
                    self.transaction_processor.total_transactions,
                    self.transaction_processor.total_fees,
                    wallets_balances,
                    self.top_wallets_generator,
                    self.wallet_updater.wallets_transactions
                )
                logger.debug(f"Finished format results for {current_hour} hour.")

                hourly_results_all.append(result_data)

                logger.debug(f"Finished processing {current_hour} hour.")    

                current_hour += timedelta(hours=1)

                if current_hour.date() != start_hour.date():
                    break            

            date_part = start_hour.strftime("%Y-%m-%d")
            output_file_path = os.path.join(self.config.CURRENT_DIRECTORY, self.config.OUTPUT_FOLDER, f"{date_part}_hourly_data.json")
            with open(output_file_path, 'w') as output_file:
                json.dump(result_data, output_file, indent=4)

            logger.info(f"Hourly data extraction completed for date {date_part}.")

        except CustomProcessingError as cpe:
            raise
        
        except Exception as e:
            CustomProcessingError.handle_processing_exception(e)      


class ExtractorFactory:
    @staticmethod
    def create_extractor(extractor_type, extract_date):
        """
        Create an extractor of the specified type for the given extraction date.

        Parameters
        ----------
        extractor_type : str
            The type of extractor to create. Must be either 'hourly' or 'daily'.
        extract_date : str
            The date for which data extraction will occur, formatted as YYYY-MM-DD.

        Returns
        -------
        extractor : BaseDataExtractor
            An instance of the specified extractor type.

        Raises
        ------
        ValueError
        CustomProcessingError

        """
        try:
            logger.info(f"Attempting to create extractor of type: {extractor_type}")

            config = Config()
            api = EtherAPI(config)
            file_manager = FileManager(config)
            block_downloader = BlockDownloader(api, file_manager)
            block_file_processor = BlockFileProcessor(block_downloader, file_manager)
            transactions_grouper = TransactionsGrouper(block_file_processor)
            transaction_processor = TransactionProcessor()
            wallet_updater = WalletUpdater()
            wallet_classifier = WalletClassifier()
            top_wallets_generator = TopWalletsGenerator()
            result_formatter = ResultFormatter()
        
            if extractor_type == 'hourly':
                extractor = HourlyDataExtractor(
                    transactions_grouper,
                    transaction_processor,
                    wallet_updater,
                    wallet_classifier,
                    top_wallets_generator,
                    result_formatter
                )
            elif extractor_type == 'daily':
                extractor = DailyDataExtractor(
                    transactions_grouper,
                    transaction_processor,
                    wallet_updater,
                    wallet_classifier,
                    top_wallets_generator,
                    result_formatter
                )
            else:
                raise ValueError(f"Invalid extractor type: {extractor_type}")  

            logger.info(f"Extractor of type {extractor_type} created successfully")
            return extractor

        except CustomProcessingError as cpe:            
            raise

        except Exception as e:            
            CustomProcessingError.handle_processing_exception(e,context="ExtractorFactory.create_extractor")
            raise


if __name__ == "__main__":
    """
    For testing the data extraction functionality.
    """
    print("Choose an option:")
    print("1: Hourly data extraction")
    print("2: Daily data extraction")

    choice = input("Choose an option (1/2): ")
    extract_date = "2024-10-02 00:00:00"

    if choice == '1':
        hourly_extractor = ExtractorFactory.create_extractor('hourly', extract_date)
        hourly_extractor.extract_data(extract_date)
    elif choice == '2':
        daily_extractor = ExtractorFactory.create_extractor('daily', extract_date)
        daily_extractor.extract_data(extract_date)
    else:
        print("Unknown mode.")