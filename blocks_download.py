from multiprocessing import cpu_count, Manager, Pool, Lock
import time
import requests 
import json
import os
from PyQt5.QtWidgets import QInputDialog
from datetime import datetime, timezone, timedelta
from config import Config
from logger import logger
from error_handler import ErrorHandler, CustomProcessingError
from typing import Any
 

class BlockInput:
    """
    Handles user input for the number of blocks to fetch, using either
    console-based input, graphical interface or other.

    Parameters
    ----------
    method : str, optional
        The input method to use (default is "console").
    """
    def __init__(self, method: str="console") -> None:
        self.method = method


    @ErrorHandler.ehd()
    def get_num_blocks_to_fetch(self, max_attempts: int=5) -> int:
        """
        Retrieves the number of blocks to fetch using the specified input method.

        Parameters
        ----------
        max_attempts : int, optional
            The maximum number of attempts for console input validation
            (default is 5). Only applicable to console input.

        Returns
        -------
        int
            The number of blocks to fetch.

        Raises
        ------
        ValueError
            If an invalid method is specified.
        """
        logger.debug(f"Attempting to get number of blocks to fetch using method: {self.method}")

        if self.method == "console":
            return self.console_input(max_attempts=max_attempts)

        elif self.method == "interface":
            return self.interface_input()

        else:
            raise ValueError("Invalid method, use 'console' or 'interface'.")


    @ErrorHandler.ehd(custom_message="Maximum number of attempts reached.", mode="console")
    def console_input(self, max_attempts: int) -> int:
        """
        Retrieves the number of blocks to fetch via the console, allowing multiple attempts.

        Returns
        -------
        int
            The number of blocks to fetch.

        Raises
        ------
        ValueError
            If the maximum number of attempts is reached.
        """
        attempts = 0
        while attempts < max_attempts:
            try:
                num_blocks = self.get_user_input()
                return self.validate_input_console(num_blocks)

            except CustomProcessingError:
                attempts += 1

        raise ValueError("Maximum number of attempts reached.")


    @ErrorHandler.ehd()
    def interface_input(self) -> int | None:
        """
        Prompts the user to enter the number of blocks to fetch via a graphical interface.

        Returns
        -------
        tuple[int, bool] or None
            The number of blocks to fetch and a confirmation flag (`True` if confirmed).
            Returns `None` if the user cancels the input.
        """
        num_blocks, ok_pressed = QInputDialog.getInt(None, "Number of Blocks", "Enter the number of blocks to fetch:")
        if ok_pressed:
            self.validate_input_interface(num_blocks)
            logger.debug(f"User input for number of blocks via interface: {num_blocks}")
            return num_blocks

        else:
            logger.debug("User cancelled input via interface.")
            return None


    @staticmethod
    @ErrorHandler.ehd(custom_message="The entered value is not an integer.", mode="console")
    def get_user_input() -> int:
        """
        Prompts the user to enter the number of blocks to fetch and attempts to convert the input to an integer.

        Returns
        -------
        int
            The number of blocks to fetch.

        Raises
        ------
        ValueError
            If the input cannot be converted to an integer.
        """
        user_input = input("Enter the number of blocks to fetch: ")
        logger.debug(f"User input: {user_input}")
        return int(user_input)

    @staticmethod
    @ErrorHandler.ehd(custom_message="Number of blocks must be greater than 0.", mode="console")
    def validate_input_console(num_blocks: int) -> int:
        """
        Validates the number of blocks specifically for console input.

        Parameters
        ----------
        num_blocks : int
            The number of blocks to validate.

        Returns
        -------
        int
            The validated number of blocks.

        Raises
        ------
        CustomProcessingError
            Raised if the number of blocks is less than or equal to 0
        """
        return BlockInput._validate_input(num_blocks)


    @staticmethod
    @ErrorHandler.ehd(custom_message="Number of blocks must be greater than 0.", mode="interface")
    def validate_input_interface(num_blocks: int) -> int:
        """
        Validates the number of blocks specifically for GUI/interface input.

        Parameters
        ----------
        num_blocks : int
            The number of blocks to validate.

        Returns
        -------
        int
            The validated number of blocks.

        Raises
        ------
        CustomProcessingError
            Raised if the number of blocks is less than or equal to 0
        """
        return BlockInput._validate_input(num_blocks)


    @staticmethod
    def _validate_input(num_blocks: int) -> int:
        """
        Core validation logic for the number of blocks.

        Parameters
        ----------
        num_blocks : int
            The number of blocks to validate.

        Returns
        -------
        int
            The validated number of blocks.

        Raises
        ------
        ValueError
            If the number of blocks is less than or equal to 0.
        """
        if num_blocks <= 0:
            raise ValueError("Number of blocks must be greater than 0.")

        return num_blocks


@ErrorHandler.ehdc()
class EtherAPI:
    """
    A class for interacting with the Ethereum blockchain via etherscanAPI.

    This class provides methods to retrieve the latest block number, block timestamps,
    and transactions for a specific block.
    
    Attributes
    ----------
    config : Config
        Configuration object containing:
        - API_URL: Base URL for the Etherscan API
        - API_KEY: Authentication key for API access
    """
    def __init__(self, config: Config) -> None:
        self.config = config


    def get_latest_block_number(self) -> int:
        """
        Fetches the latest Ethereum block number.

        Returns the number of the most recently mined block in the main chain.
        This represents the current blockchain height.

        Returns
        -------
        int
            The latest block number as an integer.
        """
        logger.debug("Requesting latest block number from Ethereum API.")

        endpoint = self._build_endpoint('proxy', 'eth_blockNumber')
        response = self._get_response(endpoint)
        result = self._parse_response(response, "result")

        Utils.check_empty_result(result, "result for latest block number")

        block_number = Utils.hex_to_int(result)

        logger.debug(f"Latest block number retrieved: {block_number}")

        return block_number


    def get_block_timestamp(self, block_number: int) -> int:
        """
        Retrieves the timestamp of a specific block.

        Parameters
        ----------
        block_number : int
            The number of the block whose timestamp is to be retrieved.

        Returns
        -------
        int
            The timestamp of the block as an integer.
        """
        logger.debug(f"Requesting block timestamp for block number: {block_number}")

        params = {
            'tag': Utils.int_to_hex(block_number),
            'boolean': 'true'
        }
        endpoint = self._build_endpoint('proxy', 'eth_getBlockByNumber', params)
        response = self._get_response(endpoint)

        result = self._parse_response(response, "result")
        Utils.check_empty_result(result, "result for block timestamp")

        timestamp = result.get("timestamp")
        Utils.check_empty_result(timestamp, "timestamp in result")

        block_timestamp = Utils.hex_to_int(timestamp)

        logger.debug(f"Block timestamp retrieved: {block_timestamp}")

        return block_timestamp


    def get_block_transactions(self, block_number: int) -> list[dict]:
        """
        Retrieves the transactions from a specific block from the Ethereum blockchain.

        Parameters
        ----------
        block_number : int
            The number of the block whose transactions are to be retrieved.

        Returns
        -------
        list[dict]
            A list of transactions as dictionaries for the specified block.
        """
        logger.debug(f"Requesting block transactions for block number: {block_number}")

        params = {
            'tag': Utils.int_to_hex(block_number),
            'boolean': 'true'
        }
        endpoint = self._build_endpoint('proxy', 'eth_getBlockByNumber', params)
        response = self._get_response(endpoint)
        result = self._parse_response(response, "result")

        Utils.check_empty_result(result, "result for block transactions")

        transactions = result.get("transactions")
        Utils.check_empty_result(transactions, "transactions in result")
        Utils.check_type(transactions, list, "transactions")

        logger.debug(f"Number of transactions retrieved for block number {block_number}: {len(transactions)}")
        return transactions


    @staticmethod
    def _get_response(endpoint: str, timeout: int | None = None) -> requests.Response:
        """
        Sends an HTTP GET request to the specified URL and handles the response.

        Parameters
        ----------
        endpoint : str
            The endpoint to which the HTTP GET request is sent.
        timeout : int, optional
            The number of seconds as int to wait for connection.

        Returns
        -------
        requests.Response
            The response object from the HTTP GET request if the request is successful.

        Raises
        ------
            - `requests.ConnectionError`: When the connection to the server fails.
            - `requests.Timeout`: When the request times out.
            - `requests.TooManyRedirects`: When too many redirects are encountered.
            - `requests.HTTPError`: When the server returns an HTTP error.
        """
        logger.debug("Sending GET request")
        response = requests.get(endpoint, timeout=timeout)
        response.raise_for_status()
        logger.debug(f"Request succeeded with status code: {response.status_code}")
        return response


    def _build_endpoint(self, module: str, action: str, params: dict = None) -> str:
        """
        Builds an API endpoint URL by appending the provided parameters to a base URL.

        Parameters
        ----------
        module : str
            The module name for the API call (e.g., 'proxy').
        action : str
            The action to be performed (e.g., 'eth_getBlockByNumber').
        params : dict, optional
            Arbitrary parameters to be included in the URL (e.g., 'tag': 'latest', 'boolean': 'true').
            If no parameters are provided, the function constructs the URL without any query parameters.

        Returns
        -------
        str
            The constructed endpoint URL.
        """
        if params is None:
            params = {}

        if not self.config.API_URL or not self.config.API_KEY:
            raise ValueError("Missing API_URL or API_KEY in configuration")

        logger.debug(f"Building endpoint with params: {params}")

        url = f"{self.config.API_URL}?module={module}&action={action}&apikey={self.config.API_KEY}"

        if params:
            query_string = "&".join(f"{key}={value}" for key, value in params.items())
            url += f"&{query_string}"

        return url


    @staticmethod
    def _parse_response(response: requests.Response, key: str) -> str | dict:
        """
        Parses the JSON response from an API request and retrieves the value associated with the specified key.

        Parameters
        ----------
        response : requests.Response
            The response object from the API request.
        key : str
            The key for which the value should be retrieved from the response JSON.

        Returns
        -------
        str | dict
            The value corresponding to the specified key, can be string or dictionary.
        """
        logger.debug(f"Parsing response, looking for key: {key}")
        return response.json().get(key)


@ErrorHandler.ehdc()
class FileManager:
    """
    A class to manage JSON file operations including saving and loading data.
    """

    @staticmethod
    def save_to_json(data: dict | list, file_path: str) -> None:
        """
        Save the provided data to a JSON file at the specified file path.

        Parameters
        ----------
        data : dict or list
            The data to be saved in JSON format.
        file_path : str
            The full path where data will be saved.
        """
        Utils.check_empty_result(data, "data to save")

        with open(file_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)  # type: ignore

        logger.debug(f"Data saved to JSON file: {file_path}")


    @staticmethod
    def load_from_json(file_path: str) -> dict | list:
        """
        Load data from a JSON file at the specified file path.

        Parameters
        ----------
        file_path : str
            The full path to the JSON file.

        Returns
        -------
        dict or list
            The data loaded from the JSON file.
        """
        with open(file_path, 'r') as file:
            data = json.load(file)

        logger.debug(f"Data loaded from JSON file: {file_path}")
        return data


    @staticmethod
    def remove_file(file_path: str) -> None:
        """
        Removes a file from the filesystem.

        Parameters
        ----------
        file_path : str
            The path of the file to be removed.
        """
        if not os.path.exists(file_path):
            logger.warning(f"File not found, skipping removal: {file_path}")
            return

        os.remove(file_path)
        logger.debug(f"Removed: {file_path}")


@ErrorHandler.ehdc()
class Utils:

    @staticmethod
    def check_fetched_blocks(block_number: int, fetched_block_numbers: list) -> bool:
        if block_number in fetched_block_numbers:
            logger.debug(f"Block {block_number} already fetched. Skipping...")
            return True
        return False

    @staticmethod
    def check_interrupt_flag(interrupt_flag, data, data_type) -> (None,int):
        if interrupt_flag and interrupt_flag.value:
            logger.info(f"Processing interrupted for {data_type}: {data}")
            return None, 0

    @staticmethod
    def check_is_negative(result: int) -> None:
        if result < 0:
                raise ValueError(f"Negative value for {result}.")

    @staticmethod
    def check_empty_result(result: Any, data_type: str) -> None:
        if not result:
                raise ValueError(f"Empty {data_type}.")

    @staticmethod
    def hex_to_int(hex_value: str) -> int:
        return int(hex_value, 16)

    @staticmethod
    def int_to_hex(int_number: int) -> str:
        return hex(int_number)

    @staticmethod
    def check_type(data: Any, expected_type: type, data_name: str) -> None:
        if not isinstance(data, expected_type):
            raise ValueError(f"Invalid {data_name}"
                             f" format: Expected {expected_type.__name__}, got {type(data).__name__}.")

@ErrorHandler.ehdc()
class BlockTimestampFinder:
    """
    A class for finding the first and last block number based on a specific date in a blockchain using a
    binary search algorithm.

    Parameters
    ----------
    ether_api : EtherAPI
        An API instance that provides methods to fetch the latest block number and block timestamps.
    """
    def __init__(self, ether_api: EtherAPI):
        self.ether_api = ether_api
        self.max_iterations = 100

    def get_timestamp_of_first_block_on_target_date(self, target_date: str) -> int:
        """
        Finds the first block number mined in a specified target date.

        Parameters
        ----------
        target_date : str
            The target date in the format 'YYYY-MM-DD'.

        Returns
        -------
        int
            The block number of the first block on the given date.
        """
        logger.info(f"Starting search for the first block on {target_date}")

        self._validate_date(target_date)

        target_timestamp = self._get_target_timestamp(target_date)

        start_block_number = self.ether_api.get_latest_block_number()
        end_block_number = 0

        initial_block_number = self._binary_search_block_for_timestamp(
            start_block_number,
            end_block_number,
            target_timestamp
        )

        first_block_number = self._find_final_block(initial_block_number, target_timestamp, block_type='first')

        logger.debug(f"First block on {target_date} is {first_block_number}")

        return first_block_number


    def get_timestamp_of_last_block_on_target_date(self, target_date: str) -> int:
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
        """
        logger.info(f"Starting search for the last block on {target_date}")

        self._validate_date(target_date)

        target_timestamp = self._get_target_timestamp(target_date, next_day=True)

        latest_block_number = self.ether_api.get_latest_block_number()

        start_block_number = latest_block_number
        end_block_number = 0

        initial_block_number = self._binary_search_block_for_timestamp(
            start_block_number,
            end_block_number,
            target_timestamp,
        )

        last_block_number = self._find_final_block(initial_block_number, target_timestamp, block_type='last')

        logger.info(f"Last block on {target_date} is {last_block_number}")
        return last_block_number


    def _binary_search_block_for_timestamp(self, start_block: int, end_block: int,
                                           target_timestamp: int) -> int:
        """
        Performs binary search to find the block number based on the target timestamp.

        Parameters
        ----------
        start_block : int
            The starting block number for the search.
        end_block : int
            The ending block number for the search.
        target_timestamp : int
            The target timestamp to compare against.

        Returns
        -------
        int
            The block number that matches the target timestamp criteria.
        """
        logger.debug(
            f"Starting binary search between blocks {start_block} and {end_block} for timestamp {target_timestamp}")

        iterations = 0
        while start_block > end_block and iterations < self.max_iterations:
            iterations += 1
            mid_block_number = (start_block + end_block) // 2
            mid_block_timestamp = self.ether_api.get_block_timestamp(mid_block_number)
            logger.debug(f"Checking block number: {mid_block_number}, Timestamp: {mid_block_timestamp}")

            if mid_block_timestamp >= target_timestamp:
                start_block = mid_block_number - 1
            else:
                end_block = mid_block_number + 1

        logger.debug(f"Binary search complete. Closest block number: {start_block}")
        return start_block


    @staticmethod
    def _validate_date(date_str: str) -> None:
        """
        Validates if the input string is a valid date in the format YYYY-MM-DD
        and checks if the date is not in the future.

        Parameters
        ----------
        date_str : str
            The date string to validate.

        Raises
        ------
        ValueError
            If the date format is incorrect or if the date is in the future.
        """
        logger.debug(f"Validating date: {date_str}")

        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        except ValueError:
            raise ValueError("Invalid date format! Date must be in the format YYYY-MM-DD.")

        current_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

        if target_date > current_date:
            raise ValueError(f"Cannot search for blocks on a future date: {date_str}")

        logger.debug(f"Successfully validated date: {date_str}")


    @staticmethod
    def _get_target_timestamp(target_date: str, next_day: bool = False) -> int:
        """
        Converts a date string to a Unix timestamp. Optionally calculates the timestamp for the next day.

        Parameters
        ----------
        target_date : str
            The target date in the format YYYY-MM-DD.
        next_day : bool, optional
            If True, calculates the timestamp for the next day (default is False).

        Returns
        -------
        int
            The Unix timestamp corresponding to the target date or the next day if `next_day` is True.
        """
        logger.debug(f"Converting target date {target_date} to timestamp.")

        if next_day:
            next_day = datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=1)
            target_timestamp = int(next_day.replace(tzinfo=timezone.utc).timestamp())
        else:
            target_timestamp = int(datetime.strptime(target_date + " 00:00", "%Y-%m-%d %H:%M")
                                   .replace(tzinfo=timezone.utc).timestamp())

        day_type = "next day's" if next_day else "today's"
        logger.debug(f"Converted {target_date} to {day_type} timestamp.")

        return target_timestamp


    def _find_final_block(self, start_block: int, target_timestamp: int, block_type: str) -> int:
        """
        Finds the final block based on timestamp conditions for either the first or last block.

        Parameters
        ----------
        start_block : int
            Initially found block number.
        target_timestamp : int
            Target timestamp to compare against.
        block_type : str
            Type of block: 'first' for the first block, 'last' for the last block.

        Returns
        -------
        int
            Adjusted block number based on timestamp comparison.
        """
        logger.debug(f"Checking block for {block_type} block.")

        found_block_timestamp = self.ether_api.get_block_timestamp(start_block)

        if block_type == 'first':
            if found_block_timestamp < target_timestamp:
                start_block += 1
                logger.debug(f"First block adjusted to {start_block}.")
            else:
                logger.debug(f"First block is fine, no adjustment needed.")

        elif block_type == 'last':
            if found_block_timestamp >= target_timestamp:
                start_block -= 1
                logger.debug(f"Last block adjusted to {start_block}.")
            else:
                logger.debug(f"Last block is fine, no adjustment needed.")
        else:
            raise ValueError(f"Unknown block type: {block_type}")

        return start_block

@ErrorHandler.ehdc()
class BlockService:
    """
    Service class for blockchain block operations.

    Attributes
    ----------
    ether_api : EtherAPI
        API for fetching data from the blockchain.
    """

    def __init__(self, ether_api: EtherAPI):
        self.ether_api = ether_api

    def fetch_block_data(self, block_number: int) -> dict:
        """
        Fetches the data of a specified block from the Ethereum API.

        Parameters
        ----------
        block_number : int
            The number of the block to fetch.

        Returns
        -------
        dict
            A dictionary containing the block data:
            - block_number : int
            - timestamp : int
            - transactions : list
        """
        timestamp = self.ether_api.get_block_timestamp(block_number)
        transactions = self.ether_api.get_block_transactions(block_number)

        return {
            "block_number": block_number,
            "timestamp": timestamp,
            "transactions": transactions
        }

    @staticmethod
    def is_block_fetched(block_number: int, fetched_block_numbers: list) -> bool:
        """
        Checks if the block has already been fetched.

        Parameters
        ----------
        block_number : int
            The number of the block to check.
        fetched_block_numbers : list
            A list of block numbers that have already been fetched.

        Returns
        -------
        bool
            True if the block has already been fetched, False otherwise.
        """
        return Utils.check_fetched_blocks(block_number, fetched_block_numbers)


@ErrorHandler.ehdc()
class BlockDownloader:
    """
    A class to download a single block data from an API and save it to JSON files.

    Parameters
    ----------
    ether_api : EtherAPI
        An API object that implements methods to fetch block timestamps and transactions.
    file_manager : FileManager
        A file manager object that implements a method for saving data to JSON files.
    config : Config
        Configuration object containing settings.
    block_service : BlockService
        Service for common block operations.
    """

    def __init__(
            self,
            ether_api: EtherAPI,
            file_manager: FileManager,
            config: Config,
            block_service: BlockService
    )       -> None:

        self.ether_api = ether_api
        self.file_manager = file_manager
        self.config = config
        self.block_service = block_service

    def download_single_block(self, block_number: int, fetched_block_numbers: list) -> None:
        """
        Downloads data for a single block from the API and saves it to a JSON file.

        Parameters
        ----------
        block_number : int
            The number of the block for which data is to be fetched.
        fetched_block_numbers : list of int
            A list of block numbers that have already been fetched.
            The block with `block_number` will be skipped if it is in this list.

        Returns
        -------
        None
            This method does not return any value.

        Raises
        ------
        Exception
            If an error occurs during the fetching of block timestamp or transactions, or while saving data to a file.
        """
        logger.debug(f"Downloading single block: {block_number}")

        if self.block_service.is_block_fetched(block_number, fetched_block_numbers):
            return

        block_data = self.block_service.fetch_block_data(block_number)

        self.file_manager.save_to_json(
            block_data,
            os.path.join(self.config.BLOCKS_DATA_DIR, f"block_{block_number}.json")
        )

        fetched_block_numbers.append(block_number)
        self.file_manager.save_to_json(fetched_block_numbers, self.config.BLOCKS_DATA_FILE)

        logger.debug(f"Single block: {block_number} download successful")


@ErrorHandler.ehdc()
class BlockProcessor:
    """
    A class to process blockchain blocks, potentially in a multiprocessing environment.

    Attributes
    ----------
    ether_api : EtherAPI
        API interface used to fetch block data.
    file_manager : FileManager
        File manager used to save block data.
    config : Config
        Configuration object containing settings.
    block_service : BlockService
        Service for common block operations.

    Methods
    -------
    process_block
    """

    def __init__(self, ether_api: EtherAPI, file_manager: FileManager, config: Config, block_service: BlockService):
        self.ether_api = ether_api
        self.file_manager = file_manager
        self.config = config
        self.block_service = block_service

    def process_block(self, block_number, fetched_block_numbers, interrupt_flag=None):
        """
        Processes a blockchain block by fetching its data, validating, and saving it to a file.
        The operation can be interrupted via the `interrupt_flag`. If the flag is set to True, processing stops early.

        Parameters
        ----------
        block_number : int
            The number of the block to process.
        fetched_block_numbers : list
            A list of block numbers that have already been processed.
        interrupt_flag : multiprocessing.Value, optional
            Flag to signal interruption of processing.

        Returns
        -------
        tuple
            A tuple containing:
            - block_number : int or None
                The number of the processed block or None if processing was skipped or interrupted.
            - result : int
                A result code indicating the status of the processing (for update progress):
                - 1: Success / Block in fetched list
        """
        Utils.check_interrupt_flag(interrupt_flag, block_number, "block_number")
        Utils.check_type(block_number, int, "block_number")
        Utils.check_is_negative(block_number)

        logger.info(f"Processing block: {block_number}")

        if self.block_service.is_block_fetched(block_number, fetched_block_numbers):
            return None, 1

        block_data = self.block_service.fetch_block_data(block_number)

        self.file_manager.save_to_json(
            block_data,
            os.path.join(self.config.BLOCKS_DATA_DIR, f"block_{block_number}.json")
        )

        logger.info(f"Processing block: {block_number} finished")

        return block_number, 1


@ErrorHandler.ehdc()
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

    def update_progress(self, x, progress_callback, total_target, fetched_block_numbers, save_callback, save_interval = 50):
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
        save_interval : int, optional
            The interval at which progress should be saved, defaults to 50

        Raises
        ------
        RuntimeError
            If an exception is raised while updating progress or invoking callbacks, a RuntimeError
            is raised.
        """
        try:
            with self.progress_lock:           
                block_number, progress_increment = x
            
                self._update_block_list(block_number, fetched_block_numbers)

                progress_value = self._increment_progress(progress_increment)
        
                if progress_callback:
                    progress_callback(total_target, progress_value)
        
                if self._should_save_progress(save_interval):
                    logger.info(f"Saving progress with {len(fetched_block_numbers)} fetched blocks.")
                    save_callback(fetched_block_numbers)

        except Exception as e:
            logger.error(f"Failed to update progress: {e}")
            raise RuntimeError("Failed to update progress") from e


    @staticmethod
    def _update_block_list(block_number, fetched_block_numbers):
        """
        Updates the list of processed blocks.

        Parameters
        ----------
        block_number : int or None
            The identifier of the block that has been processed, or None
        fetched_block_numbers : list
            The list where the block number will be added
        """
        if block_number is not None:
            fetched_block_numbers.append(block_number)
            logger.info(f"Block {block_number} added to fetched_block_numbers.")


    def _increment_progress(self, increment):
        """
        Increments the progress counter.

        Parameters
        ----------
        increment : int or None
            The value to add to the progress counter, or None if no increment

        Returns
        -------
        int
            The current value of the progress counter
        """
        if increment is not None:
            self.total_processed_blocks.value += increment
        return self.total_processed_blocks.value


    def _should_save_progress(self, interval=50):
        """
        Determines if progress should be saved based on the number of processed blocks.

        Parameters
        ----------
        interval : int, optional
            The interval at which progress should be saved, defaults to 50

        Returns
        -------
        bool
            True if progress should be saved, False otherwise
        """
        return self.total_processed_blocks.value % interval == 0

    @staticmethod
    def _create_error_callback():
        """
        Creates an error callback function for asynchronous processing.

        Returns
        -------
        callable
            A function that logs errors occurring during asynchronous processing
        """

        def error_callback(e):
            logger.error(f"Error occurred in async process: {e}")

        return error_callback


    def _create_interrupt_checker(self, check_interrupt):
        def check_interrupt_wrapper():
            if check_interrupt:
                self.interrupt_flag.value = check_interrupt()
            return self.interrupt_flag.value

        return check_interrupt_wrapper


    def _process_blocks(self, target_block_numbers, process_func, progress_callback,
                        check_interrupt_wrapper, fetched_block_numbers, save_callback, save_interval=50):
        """
        Process a sequence of blocks, checking for interrupts between each one.

        Parameters
        ----------
        target_block_numbers : list of int
            Block numbers to process
        process_func : callable
            Function to process each block
        progress_callback : callable
            Function to update progress indicators
        check_interrupt_wrapper : callable
            Function to check if processing should be interrupted
        fetched_block_numbers : list
            List to track fetched block numbers
        save_callback : callable
            Function to save progress data
        save_interval : int, optional
            Interval for saving progress, defaults to 50

        Raises
        ------
        RuntimeError
            If an error occurs during block processing
        """
        for block_number in target_block_numbers:
            try:
                if check_interrupt_wrapper():
                    logger.info("Interrupt flag is set. Stopping...")
                    break

                logger.info(f"Adding block to process: {block_number}")

                self.apply_async(
                    process_func,
                    args=(block_number, fetched_block_numbers, self.interrupt_flag),
                    callback=lambda x: self.update_progress(
                        x,
                        progress_callback,
                        len(target_block_numbers),
                        fetched_block_numbers,
                        save_callback,
                        save_interval
                    ),
                    error_callback=self._create_error_callback()
                )
                time.sleep(0.5)

            except Exception as e:
                logger.error(f"Failed to start processing block {block_number}: {e}")
                raise RuntimeError("Failed to start processing block") from e


    def start(
        self,
        target_block_numbers,
        process_func,
        progress_callback,
        check_interrupt,
        fetched_block_numbers,
        save_callback,
        save_interval=50
    ):
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
        save_interval : int, optional
            The interval at which progress should be saved, defaults to 50
        """
        try:
            check_interrupt_wrapper = self._create_interrupt_checker(check_interrupt)

            self._process_blocks(
                target_block_numbers,
                process_func,
                progress_callback,
                check_interrupt_wrapper,
                fetched_block_numbers,
                save_callback,
                save_interval
            )

        finally:
            self.pool.close()
            self.pool.join()


@ErrorHandler.ehdc()
class MainBlockProcessor:
    """
    A central class for orchestrating the processing of blockchain blocks download procedure. 
    It integrates various components such as the API, file management, 
    block downloading, and block processing, ensuring a cohesive workflow.

    Parameters
    ----------
    config : Config
        The configuration object.

    Methods
    -------
    get_target_block_numbers
    process_blocks
    handle_missing_blocks
    run
    """    
    def __init__(self, config):
        self.config = config
        self.ether_api = EtherAPI(self.config)
        self.file_manager = FileManager()
        self.block_service = BlockService(self.ether_api)
        self.block_downloader = BlockDownloader(self.ether_api, self.file_manager, self.config, self.block_service)
        self.block_processor = BlockProcessor(self.ether_api, self.file_manager, self.config, self.block_service)
    
    def get_target_block_numbers(self, block_numbers_or_num_blocks):
        """
        Retrieves target block numbers based on input type.

        Parameters
        ----------
        block_numbers_or_num_blocks : Union[list, int]
            A list of block numbers or an integer indicating the number of blocks to retrieve.
            If an integer is provided, it represents the number of latest blocks to retrieve.

        Returns
        -------
        list
            A list of target block numbers.

        Raises
        ------
        ValueError
            If input is neither a list nor an integer.

        RuntimeError
            If an error occurs while fetching the target block numbers.
        """
        try:

            if isinstance(block_numbers_or_num_blocks, list):
                logger.debug(f"MainBlockProcessor: Received list of block numbers")
                return block_numbers_or_num_blocks

            elif isinstance(block_numbers_or_num_blocks, int):
                latest_block_number = self.ether_api.get_latest_block_number()
                logger.debug(f"MainBlockProcessor: Fetched latest block number")
                target_blocks = list(
                    range(
                        latest_block_number - 1,
                        latest_block_number - block_numbers_or_num_blocks - 1,
                        -1
                    )
                )                
                logger.info(f"Generated target block numbers from {target_blocks[0]} to {target_blocks[-1]}. Total count: {len(target_blocks)}")
                return target_blocks

            else:
                raise ValueError("Invalid input: must be either a list of block numbers or an integer number of blocks")

        except ValueError as e:
            logger.error(f"MainBlockProcessor: Failed to get target block numbers: {str(e)}")
            raise  

        except Exception as e:
            logger.error(f"MainBlockProcessor: Failed to get target block numbers: {str(e)}")
            raise RuntimeError(f"MainBlockProcessor: Failed to get target block numbers: {str(e)}") from e

    def process_blocks(self, target_block_numbers, progress_callback=None, check_interrupt=None):       
        """
        Processes blocks based on target block numbers.

        Parameters
        ----------
        target_block_numbers : list
            A List of target block numbers to process.
        progress_callback : callable, optional
            A callback function to report progress (default is None).
        check_interrupt : callable, optional
            A function to check if processing should be interrupted (default is None).

        Raises
        ------
        RuntimeError
            If an error occurs during block processing.
        """
        try:
            logger.debug("MainBlockProcessor: Starting block processing")
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
                logger.info("MainBlockProcessor: Process interrupted by check_interrupt.")
                return
            
            self.handle_missing_blocks(target_block_numbers, fetched_block_numbers)
            self.file_manager.save_to_json(fetched_block_numbers, self.config.BLOCKS_DATA_FILE)
            logger.debug("MainBlockProcessor: process_blocks executed successfully")

        except Exception as e:
            logger.error(f"MainBlockProcessor: Error during block processing: {str(e)}")
            raise RuntimeError(f"MainBlockProcessor: Error during block processing: {str(e)}") from e        


    def handle_missing_blocks(self, target_block_numbers, fetched_block_numbers):
        """
        Handles the downloading of missing blocks.

        Parameters
        ----------
        target_block_numbers : list
            A List of target block numbers to check.
        fetched_block_numbers : list
            A List of block numbers that have already been fetched.

        Returns
        -------
        None

        Raises
        ------
        RuntimeError
            If an error occurs during the download of a block, the process raises a `RuntimeError`. 
        """
        missing_blocks = [block_number for block_number in target_block_numbers if block_number not in fetched_block_numbers]
        if missing_blocks:
            logger.warning(f"Missing blocks detected: {missing_blocks}")
            for block_number in missing_blocks:
                try:
                    self.block_downloader.download_single_block(block_number, fetched_block_numbers)
                    time.sleep(self.config.REQUEST_DELAY)
                    
                except Exception as e:
                    logger.error(f"Failed to download block {block_number}: {str(e)}")
                    raise RuntimeError(f"Failed to download block {block_number}") from e


    def run(self, block_numbers_or_num_blocks, progress_callback=None, check_interrupt=None):
        """
        Runs the main block processing sequence.

        Parameters
        ----------
        block_numbers_or_num_blocks : Union[list, int]
            A list of block numbers or an integer indicating the number of blocks to process.
        progress_callback : callable, optional
            A callback function to report progress (default is None).
        check_interrupt : callable, optional
            A function to check if processing should be interrupted (default is None).

        Returns
        -------
        None
        """
        logger.info("Starting MainBlockProcessor run...")
        target_block_numbers = self.get_target_block_numbers(block_numbers_or_num_blocks)
        self.process_blocks(target_block_numbers, progress_callback, check_interrupt)
        logger.info("MainBlockProcessor run completed.")       


if __name__ == "__main__":
    """
    Running the main block processor from the command line.

    Usage:
        Run the script directly to start the block processing procedure, mainly for testing.
    """    
    start_time = time.time()
    config_instance = Config()
    main_block_processor = MainBlockProcessor(config_instance)
    block_input = BlockInput()

    main_block_processor.run(
        block_input.get_num_blocks_to_fetch(),
        progress_callback=lambda total, current: print(f"Progress: {current}/{total}"),
        check_interrupt=lambda: False
    )
    end_time = time.time()
    execution_time = end_time - start_time
    print(execution_time)