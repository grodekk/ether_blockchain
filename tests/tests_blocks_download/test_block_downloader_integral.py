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


@pytest.mark.class_integral
def test_download_single_block_exception_in_save(block_downloader, mock_api, mock_file_manager, caplog):
    block_number = 20649502
    expected_timestamp = 1725118787
    expected_transactions = ['tx1', 'tx2']
    fetched_block_numbers = []

    mock_api.get_block_timestamp.return_value = expected_timestamp
    mock_api.get_block_transactions.return_value = expected_transactions

    mock_file_manager.save_to_json.side_effect = Exception("Save error")

    with caplog.at_level('DEBUG'), pytest.raises(Exception, match="Save error"):
        block_downloader.download_single_block(block_number, fetched_block_numbers)

    mock_file_manager.save_to_json.assert_called_once()

    assert block_number not in fetched_block_numbers
    assert f"Error while downloading block {block_number}" in caplog.text
    assert f"Save error" in caplog.text