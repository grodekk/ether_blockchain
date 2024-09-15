import pytest
from blocks_download import BlockDownloader

@pytest.fixture
def mock_api(mocker):
    return mocker.Mock()

@pytest.fixture
def mock_file_manager(mocker):
    return mocker.Mock()

@pytest.fixture
def block_downloader(mock_api, mock_file_manager):
    return BlockDownloader(mock_api, mock_file_manager)


class TestBlockDownloader:

    # download_single_block tests#    
    def test_download_single_block_success(self, block_downloader, mock_api, mock_file_manager, caplog):
        block_number = 20649502
        fetched_block_numbers = []
        
        mock_api.get_block_timestamp.return_value = 1725118787
        mock_api.get_block_transactions.return_value = ['tx1', 'tx2']
        
        with caplog.at_level('INFO'):
            block_downloader.download_single_block(block_number, fetched_block_numbers)
        
        mock_api.get_block_timestamp.assert_called_once_with(hex(block_number))
        mock_api.get_block_transactions.assert_called_once_with(hex(block_number))
        mock_file_manager.save_to_json.assert_called_once_with(
            {
                "block_number": block_number,
                "timestamp": 1725118787,
                "transactions": ['tx1', 'tx2']
            },
            f"block_{block_number}.json"
        )

        assert block_number in fetched_block_numbers        
        assert "Downloading single block: 20649502" in caplog.text
        assert f"Block 20649502 saved to block_{block_number}.json" in caplog.text


    def test_block_already_fetched(self, block_downloader, caplog):
        block_number = 20649502
        fetched_block_numbers = [block_number]

        with caplog.at_level('INFO'):
            block_downloader.download_single_block(block_number, fetched_block_numbers)
        
        assert f"Block {block_number} already fetched. Skipping..." in caplog.text


    def test_error_fetching_timestamp(self, block_downloader, mock_api, caplog):
        block_number = 20649502
        fetched_block_numbers = []
        
        mock_api.get_block_timestamp.side_effect = Exception("API Error fetching timestamp")

        with caplog.at_level('ERROR'):
            block_downloader.download_single_block(block_number, fetched_block_numbers)
        
        assert f"Error fetching data for block {block_number}: API Error fetching timestamp" in caplog.text


    def test_error_fetching_transactions(self, block_downloader, mock_api, caplog):
        block_number = 20649502
        fetched_block_numbers = []
        
        mock_api.get_block_timestamp.return_value = 1725118787
        mock_api.get_block_transactions.side_effect = Exception("API Error fetching transactions")

        with caplog.at_level('ERROR'):
            block_downloader.download_single_block(block_number, fetched_block_numbers)
        
        assert f"Error fetching data for block {block_number}: API Error fetching transactions" in caplog.text


    def test_error_saving_block(self, block_downloader, mock_api, mock_file_manager, caplog):
        block_number = 20649502
        fetched_block_numbers = []
        
        mock_api.get_block_timestamp.return_value = 1725118787
        mock_api.get_block_transactions.return_value = ['tx1', 'tx2']
        
        mock_file_manager.save_to_json.side_effect = Exception("File save error")

        with caplog.at_level('ERROR'):
            block_downloader.download_single_block(block_number, fetched_block_numbers)
        
        assert f"Error saving block {block_number} data to file: File save error" in caplog.text