import pytest
from unittest.mock import MagicMock
from blocks_extractor import TransactionsGrouper
from config import Config
from error_handler import CustomProcessingError

@pytest.fixture
def block_file_processor():
    return MagicMock()

@pytest.fixture
def transactions_grouper(block_file_processor):
    return TransactionsGrouper(block_file_processor)

class TestTransactionsGrouper:

    # tests group_transactions_by_hour #
    def test_group_transactions_by_hour_success(self, transactions_grouper, caplog):        
        transactions_data_1 = {
            "timestamp": 1633024800,
            "transactions": ["tx1", "tx2"]
        }
        transactions_data_2 = {
            "timestamp": 1633028400,
            "transactions": ["tx3", "tx4"]
        }

        json_file_1 = "valid_file_1.json"
        json_file_2 = "valid_file_2.json"

        transactions_grouper.config.JSON_FILES = [json_file_1, json_file_2]
        
        transactions_grouper.block_file_processor.load_block_data.side_effect = [transactions_data_1, transactions_data_2]

        with caplog.at_level('INFO'):
            result = transactions_grouper.group_transactions_by_hour()
        
        expected_result = {
            "2021-09-30 18:00:00": ["tx1", "tx2"],
            "2021-09-30 19:00:00": ["tx3", "tx4"]
        }
        assert result == expected_result
        
        assert "Total files to process: 2" in caplog.text
        assert "Total files processed: 2/2" in caplog.text
    

    def test_group_transactions_by_hour_value_error(self, transactions_grouper, caplog):
        json_file = "invalid_file.json"
        transactions_grouper.config.JSON_FILES = [json_file]
        transactions_grouper.block_file_processor.load_block_data.return_value = {"timestamp": "invalid_value"}

        with caplog.at_level('ERROR'):
            with pytest.raises(CustomProcessingError) as exc_info:
                transactions_grouper.group_transactions_by_hour()        

        assert "TransactionsGrouper.group_transactions_by_hour - Invalid value in file invalid_file.json" in caplog.text
        assert "ValueError" in str(exc_info.value)        


    def test_group_transactions_by_hour_key_error(self, transactions_grouper, caplog):
        json_file = "invalid_file.json"
        transactions_grouper.config.JSON_FILES = [json_file]
        transactions_grouper.block_file_processor.load_block_data.return_value = {"timestamp": 1633024800}

        with caplog.at_level('ERROR'):
            with pytest.raises(CustomProcessingError) as exc_info:
                transactions_grouper.group_transactions_by_hour()

        assert "TransactionsGrouper.group_transactions_by_hour - Missing key in file invalid_file.json" in caplog.text
        assert "KeyError" in str(exc_info.value)


    def test_group_transactions_by_hour_type_error(self, transactions_grouper, caplog):
        json_file = "invalid_file.json"
        transactions_grouper.config.JSON_FILES = [json_file]
        transactions_grouper.block_file_processor.load_block_data.return_value = {"timestamp": 1633024800, "transactions": 5}

        with caplog.at_level('ERROR'):
            with pytest.raises(CustomProcessingError) as exc_info:
                transactions_grouper.group_transactions_by_hour()

        assert "TransactionsGrouper.group_transactions_by_hour - Type error in file invalid_file.json" in caplog.text
        assert "TypeError" in str(exc_info.value)


    def test_group_transactions_by_hour_unexpected_error(self, transactions_grouper, caplog):
        json_file = "unexpected_file.json"
        transactions_grouper.config.JSON_FILES = [json_file]
        transactions_grouper.block_file_processor.load_block_data.side_effect = Exception("Unexpected error")

        with caplog.at_level('ERROR'):
            with pytest.raises(CustomProcessingError) as exc_info:
                transactions_grouper.group_transactions_by_hour()

        assert "TransactionsGrouper.group_transactions_by_hour - Unexpected error in file unexpected_file.json" in caplog.text
        assert "General Exception" in str(exc_info.value)