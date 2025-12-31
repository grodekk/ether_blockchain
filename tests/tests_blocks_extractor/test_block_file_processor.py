import pytest
from unittest.mock import MagicMock
from blocks_extractor import BlockFileProcessor
from error_handler import CustomProcessingError

@pytest.fixture
def mock_file_manager(mocker):
    return mocker.Mock()

@pytest.fixture
def mock_block_downloader(mocker):
    return mocker.Mock()

@pytest.fixture
def block_file_processor(mock_block_downloader, mock_file_manager):
    return BlockFileProcessor(mock_block_downloader, mock_file_manager)

class TestBlockFileProcessor:    

    # tests load_block_data #
    def test_load_block_data_success(self, block_file_processor, mock_file_manager, caplog):
        json_file = "block_123.json"
        mock_file_manager.load_from_json.return_value = {"block": 123}

        with caplog.at_level("INFO"):
            block_data = block_file_processor.load_block_data(json_file)
        
        mock_file_manager.load_from_json.assert_called_once_with(json_file)

        assert block_data == {"block": 123}
        assert "File" not in caplog.text 


    def test_load_block_data_empty_file_fetch_success(self, block_file_processor,
                                                      mock_file_manager, mock_block_downloader, caplog):
        json_file = "block_123.json"
        mock_file_manager.load_from_json.side_effect = [None, {"block": 123}]
        mock_block_downloader.download_single_block = MagicMock()

        with caplog.at_level("INFO"):
            block_data = block_file_processor.load_block_data(json_file)
        
        mock_file_manager.load_from_json.assert_any_call(json_file) 
        mock_block_downloader.download_single_block.assert_called_once_with(123, [])

        assert block_data == {"block": 123}
        assert "File block_123.json is empty or corrupted. Attempting to fetch missing data." in caplog.text

   
    def test_load_block_data_invalid_filename(self, block_file_processor, mock_file_manager, caplog):
        json_file = "invalid_filename.json"
        mock_file_manager.load_from_json.side_effect = ValueError("Invalid block number format")

        with caplog.at_level('ERROR'):
            with pytest.raises(CustomProcessingError) as exc_info:
                block_file_processor.load_block_data(json_file)
                
        assert "Invalid block number format" in caplog.text
        

    def test_load_block_data_unexpected_error(self, block_file_processor, mock_file_manager, caplog):
        json_file = "block_20649502.json"
        mock_file_manager.load_from_json.side_effect = Exception("Some unexpected error")

        with caplog.at_level('ERROR'):
            with pytest.raises(CustomProcessingError) as exc_info:
                block_file_processor.load_block_data(json_file)
        
        assert "Some unexpected error" in caplog.text