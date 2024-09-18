from multiprocessing import cpu_count, Manager, Pool, Lock
from queue import Empty
import time
import requests 
import json
import os
from PyQt5.QtWidgets import QInputDialog
from datetime import datetime, timezone, timedelta
from config import Config
import logging
from logger import LoggerConfig

logger = LoggerConfig(log_file='blocks_download.log', log_level=logging.DEBUG).get_logger()

class BlockInput:
    """
    A class used to represent the process of getting the number of blocks to fetch.

    Methods
    -------
    get_num_blocks_to_fetch
    """
    
    @staticmethod
    def get_num_blocks_to_fetch(method="console", max_attempts=5):
        """
        Prompt the user to enter the number of blocks to fetch using the specified method.

        Parameters
        ----------
        method : str, optional
            The method to use for input. Can be 'console' or 'interface' (default is 'console').
        max_attempts : int, optional
            The maximum number of attempts allowed for invalid input (default is 5).

        Returns
        -------
        int
            The number of blocks to fetch if valid input is provided.
        None
            If the user cancels the input via the 'interface' method.

        Raises
        ------
        ValueError
            If the user provides invalid input, exceeds the maximum number of attempts, or chooses an invalid method.
        """
        logger.info(f"Attempting to get number of blocks to fetch using method: {method}")              
        attempts = 0
        
        if method == "console":
            while attempts < max_attempts:
                try:
                    user_input = input("Enter the number of blocks to fetch: ")
                    num_blocks = int(user_input)
                    if num_blocks <= 0:
                        raise ValueError("Number of blocks must be greater than 0.")         

                    logger.info(f"User input for number of blocks: {num_blocks}")               
                    return num_blocks

                except ValueError as e:                
                    logger.error(f"Invalid input, please enter an integer! ({e})")
                    attempts += 1                   

            logger.error("Maximum number of attempts reached.")
            raise ValueError("Maximum number of attempts reached.")                                                   
                
        elif method == "interface":
            try:
                num_blocks, ok_pressed = QInputDialog.getInt(None, "Number of Blocks", "Enter the number of blocks to fetch:")
                if ok_pressed:
                    if num_blocks <= 0:
                         raise ValueError("Number of blocks must be greater than 0.")     

                    logger.info(f"User input for number of blocks via interface: {num_blocks}")
                    return num_blocks, True

                else:
                    logger.info("User cancelled input via interface.")
                    return None

            except Exception as e:
                logger.error(f"Problem with interface: {e}")
                raise
                
        else:
            error_message = "Invalid method, use 'console' or 'interface'."
            logger.error(error_message)
            raise ValueError(error_message)


class EtherAPI:
    """
    A class for interacting with the Ethereum blockchain via etherscanAPI.

    This class provides methods to retrieve information from the Ethereum blockchain,
    including the latest block number, block timestamps, and transactions for a specific block.
    
    Attributes
    ----------
    config : object
        An object containing configuration settings, including API URL and API key.

    Methods
    -------
    _get_response
    get_latest_block_number
    get_block_timestamp
    get_block_transactions
    """    
    def __init__(self, config):
        self.config = config 

    def _get_response(self, url):
        """
        Sends an HTTP GET request to the specified URL and handles the response.

        - This private method sends a request to the given URL and processes the HTTP response. 
        - It logs information about the request and response, and raises exceptions in case
          of HTTP errors or request failures.
        - It is intended for internal use within the `EtherAPI` class to facilitate 
          communication with the API.

        Parameters
        ----------
        url : str
            The URL to which the HTTP GET request is sent.

        Returns
        -------
        requests.Response
            The response object from the HTTP GET request if the request is successful.

        Raises
        ------
        ConnectionError
            If the HTTP response status code indicates an error (status code >= 400), or if other request exception occurs.
        """
        try:            
            response = requests.get(url)     
            logger.info(f"Received response with status code: {response.status_code}")      
            if response.status_code >= 400:               
                logger.error(f"HTTP error occurred: {response.status_code} - {response.reason}")
                raise ConnectionError(f"HTTP error occurred: {response.status_code} - {response.reason}")
            
            return response            
            
        except requests.RequestException as e:           
            logger.error(f"Request failed: {e}")
            raise ConnectionError(f"Request failed: {e}") from e
    

    def get_latest_block_number(self):
        """
        Fetches the latest Ethereum block number using the configured API.

        Returns
        -------
        int
            The latest block number as an integer.

        Raises
        ------
        ValueError
            If the API response cannot be parsed as JSON.
            If the API response is empty, or if the block number format in the API response
            is invalid and cannot be converted from hexadecimal to an integer.
        """
        url = f"{self.config.API_URL}?module=proxy&action=eth_blockNumber&apikey={self.config.API_KEY}" 
        response = self._get_response(url)

        try:
            data = response.json()           

        except ValueError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            raise ValueError("Failed to parse JSON response.") from e
        
        result = data.get("result", None)

        if not result:
            logger.error("Empty result for block number in API response.")
            raise ValueError("Empty result for block number.")

        try:
            block_number = int(result, 16) 
            logger.info(f"Latest block number retrieved: {block_number}") 
            return block_number

        except ValueError as e:
            logger.error(f"Invalid block number format: {result}")
            raise ValueError(f"Invalid block number format: {result}") from e


    def get_block_timestamp(self, block_number):
        """
        Retrieves the timestamp of a specific block from the Ethereum blockchain.

        This method sends a request to the Ethereum API to retrieve the block details
        and extracts the timestamp from the response. It converts the timestamp from
        hexadecimal format to an integer.

        Parameters
        ----------
        block_number : int
            The number of the block whose timestamp is to be retrieved.

        Returns
        -------
        int
            The timestamp of the block as an integer.

        Raises
        ------
        ValueError
            If the response cannot be parsed as JSON.   
            If the API response is empty, or if the timestamp format in the API response
            is invalid and cannot be converted from hexadecimal to an integer.                     
        """
        url = f"{self.config.API_URL}?module=proxy&action=eth_getBlockByNumber&tag={block_number}&boolean=true&apikey={self.config.API_KEY}"      
        logger.info(f"Requesting block timestamp for block number: {block_number}")
        response = self._get_response(url)           

        try:
            data = response.json()            

        except ValueError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            raise ValueError("Failed to parse JSON response.") from e 

        result = data.get("result", {})
        timestamp = result.get("timestamp", None)
        
        if not result:
            logger.error("Empty result for block timestamp in API response.")
            raise ValueError("Empty result for block timestamp.")
        
        if not timestamp:
            logger.error("Empty timestamp in result.")
            raise ValueError("Empty timestamp in result.")
        
        try:
            block_timestamp = int(timestamp, 16)
            logger.info(f"Block timestamp retrieved: {block_timestamp}")
            return block_timestamp
        
        except ValueError as e:
            logger.error(f"Invalid timestamp format: {timestamp}")
            raise ValueError(f"Invalid timestamp format: {timestamp}") from e            
    
    def get_block_transactions(self, block_number):
        """
        Retrieves the transactions from a specific block from the Ethereum blockchain.

        This method sends a request to the Ethereum API to retrieve the list of
        transactions from the response.

        Parameters
        ----------
        block_number : int
            The number of the block whose transactions are to be retrieved.

        Returns
        -------
        list
            A list of transactions for the specified block.

        Raises
        ------
        ValueError
            If the API response is empty, or if the transactions format in the API response
            is invalid or cannot be processed as a list, or if there are no transactions in the result.
        """
        url = f"{self.config.API_URL}?module=proxy&action=eth_getBlockByNumber&tag={block_number}&boolean=true&apikey={self.config.API_KEY}"        
        logger.info(f"Requesting block transactions for block number: {block_number}")
        response = self._get_response(url)
        
        try:
            data = response.json()            

        except ValueError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            raise ValueError("Failed to parse JSON response.") from e 

        result = data.get("result", {})
        transactions = result.get("transactions", [])
        
        if not result:
            logger.error("Empty result for block transactions in API response.")
            raise ValueError("Empty result for block transactions.")
        
        if not isinstance(transactions, list):
            logger.error(f"Invalid transactions format: Expected list, got {type(transactions).__name__}.")
            raise ValueError(f"Invalid transactions format: Expected list, got {type(transactions).__name__}.")
        
        if not transactions:
            logger.error("Empty transactions for transactions result.")
            raise ValueError("Empty transactions for transactions result.")
        
        logger.info(f"Number of transactions retrieved for block number {block_number}: {len(transactions)}")
        return transactions


class FileManager:
    """
    A class to manage JSON file operations including saving and loading data.

    Parameters
    ----------
    config : object
        Configuration object containing settings including file paths.

    Methods
    ----------
    _get_file_path
    save_to_json
    load_from_json
    """
    def __init__(self, config):
        self.config = config

    def _get_file_path(self, filename):
        """
        Construct the full file path for the given filename using the config object.

        Parameters
        ----------
        filename : str
            The name of the file.

        Returns
        -------
        str
            The full path to the file.
        """
        return os.path.join(self.config.BLOCKS_DATA_DIR, filename)

    def save_to_json(self, data, filename):
        """
        Save the provided data to a JSON file at the location specified by the filename.

        Parameters
        ----------
        data : object
            The data to be saved in JSON format.
        filename : str
            The name of the file where data will be saved.

        Raises
        ------
        ValueError
            If provided data is empty.
        OSError
            If there is an error saving the data to the file.
        """
        file_path = self._get_file_path(filename)
        if not data:
            logger.error("Cannot save empty data to JSON file.")
            raise ValueError("Cannot save empty data to JSON file.")
        try:
            with open(file_path, 'w') as json_file:
                json.dump(data, json_file, indent=4)
            logger.info(f"Block data saved to JSON file: {file_path}")

        except OSError as e:
            logger.error(f"Failed to save data to {file_path}: {e}")
            raise OSError("Failed to save data") from e

    def load_from_json(self, filename):
        """
        Load data from a JSON file at the location specified by the path and filename.

        Parameters
        ----------
        filename : str
            The name of the file to load data from.

        Returns
        -------
        object
            The data loaded from the JSON file.

        Raises
        ------
        FileNotFoundError
            If the file does not exist.
        ValueError
            If the JSON format is invalid.
        OSError
            If there is an error loading data from the file.
        """
        file_path = self._get_file_path(filename)

        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
            logger.info(f"Data loaded from JSON file: {file_path}")
            return data

        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Raising an exception.")
            raise FileNotFoundError("File not found.")

        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from file {file_path}: {e}")
            raise ValueError("Invalid JSON format.") from e 

        except OSError as e:
            logger.error(f"Failed to load data from {file_path}: {e}")
            raise OSError("Failed to load data") from e


class BlockTimestampFinder:
    """
    A class for finding the first and last block timestamps on a specific date in a blockchain using a binary search algorithm.

    Parameters
    ----------
    api : object
        An API instance that provides methods to fetch the latest block number and block timestamps.

    Methods
    -------
    get_timestamp_of_first_block_on_target_date  
    get_timestamp_of_last_block_on_target_date
    """
    def __init__(self, api):
        self.api = api

    def _validate_date(self, date_str):
        """
        This private method validates if the input string is a valid date in the format YYYY-MM-DD.

        Parameters
        ----------
        date_str : str
            The date string to validate.

        Returns
        -------
        bool
            True if the date string is valid, False otherwise.

        Raises
        ------
        ValueError
            If the date format is invalid.
        """
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return True

        except ValueError:
            return False

    def get_timestamp_of_first_block_on_target_date(self, target_date):
        """
        Finds the block number of the first block mined on the specified target date.

        Parameters
        ----------
        target_date : str
            The target date in the format YYYY-MM-DD.

        Returns
        -------
        int
            The block number of the first block on the target date.

        Raises
        ------
        ValueError
            If the date format is invalid.
        RuntimeError
            If there is an error initializing the search or fetching the block timestamp.
        """
        if not self._validate_date(target_date):
            logger.error(f"Invalid target date format: {target_date}")
            raise ValueError("Date must be in the format YYYY-MM-DD")

        logger.info(f"Starting search for the first block on {target_date}")
        
        try:
            target_timestamp = int(datetime.strptime(target_date + " 00:00", "%Y-%m-%d %H:%M")
                                   .replace(tzinfo=timezone.utc).timestamp())
            latest_block_number = self.api.get_latest_block_number()

        except Exception as e:
            logger.error(f"Error initializing search: {e}")
            raise RuntimeError("Failed to initialize the search for the first block timestamp.") from e
        
        start_block_number = latest_block_number
        end_block_number = 0
        
        while start_block_number > end_block_number:
            mid_block_number = (start_block_number + end_block_number) // 2
            
            try:
                mid_block_timestamp = self.api.get_block_timestamp(hex(mid_block_number))
                logger.debug(f"Checking block number: {mid_block_number}, Timestamp: {mid_block_timestamp}")

            except Exception as e:
                logger.error(f"Error fetching block timestamp for block {mid_block_number}: {e}")
                raise RuntimeError(f"Failed to fetch block timestamp for block {mid_block_number}.") from e
            
            if mid_block_timestamp > target_timestamp:
                start_block_number = mid_block_number - 1
            else:
                end_block_number = mid_block_number + 1
        
        try:
            final_block_timestamp = self.api.get_block_timestamp(hex(start_block_number))
            logger.debug(f"Final check for block number: {start_block_number}, Timestamp: {final_block_timestamp}")

        except Exception as e:
            logger.error(f"Error fetching final block timestamp for block {start_block_number}: {e}")
            raise RuntimeError(f"Failed to fetch final block timestamp for block {start_block_number}.") from e
        
        if final_block_timestamp < target_timestamp:
            start_block_number += 1
        
        logger.info(f"First block on {target_date} is {start_block_number}")
        return start_block_number
  

    def get_timestamp_of_last_block_on_target_date(self, target_date):   
        """
        Finds the block number of the last block mined on the specified target date.

        Parameters
        ----------
        target_date : str
            The target date in the format YYYY-MM-DD.

        Returns
        -------
        int
            The block number of the last block on the target date.

        Raises
        ------
        ValueError
            If the date format is invalid.
        RuntimeError
            If there is an error initializing the search or fetching the block timestamp.
        """
        if not self._validate_date(target_date):
            logger.error(f"Invalid target date format: {target_date}")
            raise ValueError("Date must be in the format YYYY-MM-DD")

        logger.info(f"Starting search for the last block on {target_date}")

        try:
            next_day = datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=1)
            target_timestamp = int(next_day.replace(tzinfo=timezone.utc).timestamp())
            latest_block_number = self.api.get_latest_block_number()

        except Exception as e:
            logger.error(f"Error initializing search: {e}")
            raise RuntimeError("Failed to initialize the search for the last block timestamp.") from e

        start_block_number = latest_block_number
        end_block_number = 0

        while start_block_number > end_block_number:
            mid_block_number = (start_block_number + end_block_number) // 2

            try:
                mid_block_timestamp = self.api.get_block_timestamp(hex(mid_block_number))
                logger.debug(f"Checking block number: {mid_block_number}, Timestamp: {mid_block_timestamp}")

            except Exception as e:
                logger.error(f"Error fetching block timestamp for block {mid_block_number}: {e}")
                raise RuntimeError(f"Failed to fetch block timestamp for block {mid_block_number}.") from e

            if mid_block_timestamp >= target_timestamp:
                start_block_number = mid_block_number - 1
            else:
                end_block_number = mid_block_number + 1

        try:
            final_block_timestamp = self.api.get_block_timestamp(hex(start_block_number))
            logger.debug(f"Final check for block number: {start_block_number}, Timestamp: {final_block_timestamp}")

        except Exception as e:
            logger.error(f"Error fetching final block timestamp for block {start_block_number}: {e}")
            raise RuntimeError(f"Failed to fetch final block timestamp for block {start_block_number}.") from e

        if final_block_timestamp >= target_timestamp:
            start_block_number -= 1

        logger.info(f"Last block on {target_date} is {start_block_number}")
        return start_block_number
        

class BlockDownloader:
    """
    A class to download a single block data from an API and save it to JSON files.

    Parameters
    ----------
    api : object
        An API object that implements methods to fetch block timestamps and transactions.
    file_manager : object
        A file manager object that implements a method for saving data to JSON files.

    Methods
    -------
    download_single_block
    """
    def __init__(self, api, file_manager):
        self.api = api
        self.file_manager = file_manager


    def download_single_block(self, block_number, fetched_block_numbers):        
        """
        Downloads data for a single block from the API and saves it to a JSON file.

        If the block has already been fetched, it is skipped. Fetches both the timestamp and transactions for the block,
        and in case of errors, logs the failure and exits. On success, saves the block data to a file and updates the
        list of fetched blocks.

        Parameters
        ----------
        block_number : int
            The number of the block for which data is to be fetched.
        fetched_block_numbers : list of int
            A list of block numbers that have already been fetched. The block with `block_number` will be skipped if it is in this list.

        Returns
        -------
        None
            This method does not return any value.

        Raises
        ------
        Exception
            If an error occurs during the fetching of block timestamp or transactions, or while saving data to a file.   
        """
        logger.info(f"Downloading single block: {block_number}")

        if block_number in fetched_block_numbers:
            logger.info(f"Block {block_number} already fetched. Skipping...")
            return

        try:
            timestamp = self.api.get_block_timestamp(hex(block_number))     
            transactions = self.api.get_block_transactions(hex(block_number))

        except Exception as e:
            logger.error(f"Error fetching data for block {block_number}: {str(e)}")
            return

        block_data = {
            "block_number": block_number,
            "timestamp": timestamp,
            "transactions": transactions
        }

        file_path = f"block_{block_number}.json"
        
        try:
            self.file_manager.save_to_json(block_data, file_path)
            logger.info(f"Block {block_number} saved to {file_path}")
            fetched_block_numbers.append(block_number)

        except Exception as e:
            logger.error(f"Error saving block {block_number} data to file: {str(e)}")


class BlockProcessor:
    """
    A class to process blockchain blocks, potentially in a multiprocessing environment.

    Attributes
    ----------
    api : object
        API interface used to fetch block data.
    file_manager : object
        File manager used to save block data.
    config : object
        Configuration object containing settings.

    Methods
    -------
    process_block
    """
    def __init__(self, api, file_manager, config): 
        self.api = api
        self.file_manager = file_manager
        self.config = config

    def process_block(self, block_number, fetched_block_numbers, interrupt_flag=None):
        """
        Processes a block by fetching its number, timestamp and transactions, validating the data,
        saving the block data to a file, and updating the list of fetched blocks. The function also respects
        the configured request delay to avoid overloading the API.

        The operation can be interrupted based on the `interrupt_flag`. If the `interrupt_flag` is set and 
        its value is True, the processing will be halted, and the function will return early.

        Parameters
        ----------
        block_number : int
            The number of the block to process.        

        fetched_block_numbers : list
            List of block numbers that have already been processed.

        interrupt_flag : multiprocessing.Value, optional
            Flag to signal interruption of processing.

        Returns
        -------
        tuple
            A tuple containing:

            - block_number : int or None
                The number of the processed block or None if processing was skipped or interrupted.

            - result : int
                A result code indicating the status of the processing ( for update progress):
                - 1: Success
                - 0: Processing interrupted        
        
        Raises
        ------
        ValueError
            If block number, timestamp, or transactions are invalid.
        RuntimeError
            For unexpected errors during processing.       
        """
        if not isinstance(block_number, int) or block_number < 0:
            logger.error(f"Invalid block number: {block_number}")
            raise ValueError(f"Invalid block number: {block_number}")

        if interrupt_flag and interrupt_flag.value:
            logger.info(f"Processing interrupted for block: {block_number}")
            return None, 0        
            
        logger.info(f"Processing block: {block_number}")
            
        if block_number in fetched_block_numbers:
            logger.info(f"Block {block_number} already fetched. Skipping...")
            return None, 1

        try:
            timestamp = self.api.get_block_timestamp(hex(block_number))            
            if not isinstance(timestamp, int) or timestamp <= 0:
                logger.error(f"Invalid timestamp for block: {block_number}")
                raise ValueError(f"Invalid timestamp for block: {block_number}")           

            transactions = self.api.get_block_transactions(hex(block_number))
            if not all(isinstance(tx, dict) and 'hash' in tx for tx in transactions):
                logger.error(f"Invalid transactions for block: {block_number}")
                raise ValueError(f"Invalid transactions for block: {block_number}")

            block_data = {
                "block_number": block_number,
                "timestamp": timestamp,
                "transactions": transactions
            }
            
            self.file_manager.save_to_json(block_data, f"block_{block_number}.json")
            logger.info(f"Block {block_number} saved to block_{block_number}.json")           
            time.sleep(self.config.REQUEST_DELAY)                     

            return block_number, 1   

        except ValueError as e:
            logger.error(f"Validation error in process_block for block {block_number}: {str(e)}")
            raise ValueError(f"Validation error for block {block_number}") from e

        except Exception as e:
            logger.error(f"Unexpected error occurred in process_block for block {block_number}: {str(e)}")
            raise RuntimeError(f"Error while processing block {block_number}") from e

        return None, 0
    

class MultiProcessor:
    """
    A class for managing and processing blocks of data using multiprocessing.

    This class uses a pool of worker processes to handle asynchronous block processing tasks.
        
    Attributes
    ----------
    num_processes : int
        Number of worker processes in the pool, calculated as 75% of the total number of CPU cores.
    pool : multiprocessing.Pool
        A pool of worker processes for handling asynchronous tasks.
    manager : multiprocessing.Manager
        A manager object for sharing state between processes.    
    interrupt_flag : multiprocessing.Value
        A flag to indicate whether an interrupt signal has been received.
    total_processed_blocks : multiprocessing.Value
        A counter for tracking the total number of processed blocks.
    progress_lock : multiprocessing.Lock
        A lock for synchronizing access to progress updates.

    Methods
    -------
    apply_async
    update_progress
    start
    """
    def __init__(self):
        num_cores = cpu_count()
        self.num_processes = int(num_cores * 0.75)
        self.pool = Pool(processes=self.num_processes)
        self.manager = Manager()        
        self.interrupt_flag = self.manager.Value('b', False)
        self.total_processed_blocks = self.manager.Value('i', 0)
        self.progress_lock = Lock()

    def apply_async(self, func, args, callback=None, error_callback=None):     
        """
        Apply a function asynchronously using a pool of worker processes.

        This method submits a function to be executed asynchronously by the pool of worker processes.
        It provides mechanisms to handle errors and callbacks for when the function completes.

        Parameters
        ----------
        func : callable
            The function to be executed asynchronously.
        args : tuple
            The arguments to pass to the `func` when it is called.
        callback : callable, optional
            A function to be called when the asynchronous execution completes successfully.            
        error_callback : callable, optional
            A function to be called if an error occurs during the asynchronous execution.            

        Raises
        ------
        RuntimeError
            If an exception is raised during the asynchronous function submission and no
            `error_callback` is provided, a RuntimeError is raised.
        """               
        try:
            self.pool.apply_async(func, args=args, callback=callback, error_callback=error_callback)

        except Exception as e:            
            if error_callback:
                error_callback(e)    
            else:
                logger.error(f"Failed to apply async function: {e}")
                raise RuntimeError("Failed to apply async function") from e

    def update_progress(self, x, progress_callback, total_target, fetched_block_numbers, save_callback):        
        """       
        Updates the progress of block processing, updates the list of fetched blocks, and triggers callbacks.

        Parameters
        ----------
        x : tuple
            A tuple containing:
            - block_number (int or None): The identifier of the block that has been processed, or None if no block was processed.
            - progress_increment (int or None): The increment value that will be added to the total progress. If None, no increment is applied.
        progress_callback : callable
            A function that is called to update progress indicators. It should accept two arguments:
            - total_target (int): The total number of blocks that are to be processed.
            - progress_value (int): The current total number of processed blocks.
            This function is used to update progress displays or other indicators reflecting the processing progress.
        total_target : int
            The total number of blocks to process. This value is used by `progress_callback` to calculate the progress ratio.
        fetched_block_numbers : list
            A list where the numbers of processed blocks are appended. This list is updated to keep track of all blocks that have been processed.
        save_callback : callable
            A function that takes one argument:
            - fetched_block_numbers (list): The list of block numbers that have been fetched and processed so far.
            This function is called to perform periodic actions, such as saving progress data.

        Raises
        ------
        RuntimeError
            If an exception is raised while updating progress or invoking callbacks, a RuntimeError
            is raised.
        """
        try:
            with self.progress_lock:           
                block_number, progress_increment = x
            
                if block_number is not None:                    
                    fetched_block_numbers.append(block_number)
                    logger.info(f"Block {block_number} added to fetched_block_numbers.")
            
                self.total_processed_blocks.value += progress_increment if progress_increment is not None else 0
                progress_value = self.total_processed_blocks.value
        
                if progress_callback:
                    progress_callback(total_target, progress_value)
        
                if self.total_processed_blocks.value % 50 == 0:
                    logger.info(f"Saving progress with {len(fetched_block_numbers)} fetched blocks.")
                    save_callback(fetched_block_numbers)

        except Exception as e:
            logger.error(f"Failed to update progress: {e}")
            raise RuntimeError("Failed to update progress") from e

    def start(self, target_block_numbers, process_func, progress_callback, check_interrupt, fetched_block_numbers, save_callback):           
        """
        Starts the asynchronous processing of target block numbers using a multiprocessing pool.

        Parameters
        ----------
        target_block_numbers : list of int
            A list of block numbers that need to be processed.
        process_func : callable
            A function responsible for processing each block. It should accept the following arguments:
            - block_number (int): The number of the block to process.            
            - fetched_block_numbers (list): A list to track fetched block numbers.
            - interrupt_flag (multiprocessing.Value): A shared flag indicating whether to stop processing.
        progress_callback : callable
            A function that is called periodically to update the progress of the block processing. It should accept:
            - total_target (int): The total number of blocks to process.
            - progress_value (int): The current progress value (number of processed blocks).
        check_interrupt : callable or None
            A function that checks if the processing should be interrupted. It should return `True` if an interruption is requested
        fetched_block_numbers : list of int
            A list where the numbers of successfully processed blocks will be appended.
        save_callback : callable
            A function that is called to save the fetched block numbers periodically. It accepts one argument:
            - fetched_block_numbers (list): A list of block numbers that have been processed so far.

        Raises
        ------
        RuntimeError
            Raised if an error occurs during the block processing or when an interrupt check fails.
        
        Internal Functions
        -------------------
        error_callback(e)
            Handles errors that occur during asynchronous processing. Logs the error message.

        check_interrupt_wrapper()
            Checks whether the processing should be interrupted. It uses the `check_interrupt` function and updates
            the `interrupt_flag` accordingly. If an error occurs during this check, it logs the error and raises a `RuntimeError`.
        """
        def error_callback(e):
            logger.error(f"Error occurred in async process: {e}")

        def check_interrupt_wrapper():
            try:
                if check_interrupt:
                    self.interrupt_flag.value = check_interrupt()
                return self.interrupt_flag.value
                
            except Exception as e:
                logger.error(f"Check interrupt failed with error: {e}")
                raise RuntimeError("Interrupt check failed") from e

        for block_number in target_block_numbers:
            try:
                if check_interrupt_wrapper():
                    logger.info("Interrupt flag is set. Stopping...")
                    break

                logger.info(f"Adding block to process: {block_number}")

                self.apply_async(
                    process_func,
                    args=(block_number, fetched_block_numbers, self.interrupt_flag),
                    callback=lambda x: self.update_progress(x, progress_callback, len(target_block_numbers), fetched_block_numbers, save_callback),
                    error_callback=error_callback
                )
                time.sleep(0.5)

            except Exception as e:
                logger.error(f"Failed to start processing block {block_number}: {e}")
                raise RuntimeError("Failed to start processing block") from e

        self.pool.close()
        self.pool.join()


class MainBlockProcessor:
    def __init__(self, config):
        self.config = config
        self.api = EtherAPI(self.config)
        self.file_manager = FileManager(self.config)
        self.block_downloader = BlockDownloader(self.api, self.file_manager)
        self.block_processor = BlockProcessor(self.api, self.file_manager, self.config)
               
        if not os.path.exists(self.config.BLOCKS_DATA_DIR):
            os.makedirs(self.config.BLOCKS_DATA_DIR)

    def get_target_block_numbers(self, block_numbers_or_num_blocks):
        latest_block_number = self.api.get_latest_block_number()

        if isinstance(block_numbers_or_num_blocks, list):
            return block_numbers_or_num_blocks
        elif isinstance(block_numbers_or_num_blocks, int):
            return list(
                range(
                    latest_block_number - 1,
                    latest_block_number - block_numbers_or_num_blocks - 1,
                    -1
                )
            )
        else:
            raise ValueError("Invalid input: must be either a list of block numbers or an integer number of blocks")

    def process_blocks(self, target_block_numbers, progress_callback=None, check_interrupt=None):
        processor = MultiProcessor()
        fetched_block_numbers = self.file_manager.load_from_json(self.config.BLOCKS_DATA_FILE)
        process_block = self.block_processor.process_block
        
        processor.start(
            target_block_numbers=target_block_numbers,
            process_func=process_block,
            progress_callback=progress_callback,
            check_interrupt=check_interrupt,
            fetched_block_numbers=fetched_block_numbers,
            save_callback=lambda fetched: self.file_manager.save_to_json(fetched, self.config.BLOCKS_DATA_FILE)
        )

        time.sleep(1)

        if check_interrupt and check_interrupt():
            return
        
        self.handle_missing_blocks(target_block_numbers, fetched_block_numbers)
        
        self.file_manager.save_to_json(fetched_block_numbers, self.config.BLOCKS_DATA_FILE)
        print('Fetched blocks data saved')

    def handle_missing_blocks(self, target_block_numbers, fetched_block_numbers):
        missing_blocks = [block_number for block_number in target_block_numbers if block_number not in fetched_block_numbers]
        if missing_blocks:
            print(f"Missing blocks detected: {missing_blocks}")
            for block_number in missing_blocks:
                self.block_downloader.download_single_block(block_number, fetched_block_numbers)
                time.sleep(self.config.REQUEST_DELAY)

    def run(self, block_numbers_or_num_blocks, progress_callback=None, check_interrupt=None):
        target_block_numbers = self.get_target_block_numbers(block_numbers_or_num_blocks)
        self.process_blocks(target_block_numbers, progress_callback, check_interrupt)


if __name__ == "__main__":
    config = Config()
    main_block_processor = MainBlockProcessor(config)       
    block_numbers_or_num_blocks = BlockInput.get_num_blocks_to_fetch()   
    main_block_processor.run(
        block_numbers_or_num_blocks,
        progress_callback=lambda total, current: print(f"Postęp: {current}/{total}"),
        check_interrupt=lambda: False  
    )