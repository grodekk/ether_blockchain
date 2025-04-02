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


# DOWNLOAD_SINGLE_BLOCK #
@pytest.mark.unit
def test_download_single_block_success(block_downloader, mock_api, mock_file_manager, mocker, caplog):
    block_number = 20649502
    hex_block_number = 0x13b161e
    expected_transactions = ['tx1', 'tx2']
    expected_timestamp = 1725118787
    expected_file_path = f"block_{block_number}.json"
    expected_block_data = {
        "block_number": block_number,
        "timestamp": expected_timestamp,
        "transactions": expected_transactions
    }
    fetched_block_numbers = []

    mocker.patch.object(BlockDownloader, 'check_fetched_blocks', return_value=False)
    mock_api.get_block_timestamp.return_value = expected_timestamp
    mock_api.get_block_transactions.return_value = expected_transactions

    with caplog.at_level('DEBUG'):
        block_downloader.download_single_block(block_number, fetched_block_numbers)

    mock_api.get_block_timestamp.assert_called_once_with(hex_block_number)
    mock_api.get_block_transactions.assert_called_once_with(hex_block_number)
    mock_file_manager.save_to_json.assert_called_once_with(expected_block_data, expected_file_path)

    assert block_number in fetched_block_numbers
    assert "Downloading single block: 20649502" in caplog.text
    assert f"Single block: {block_number} download successful" in caplog.text


@pytest.mark.unit
def test_download_single_block_skipped(block_downloader, mock_api, mock_file_manager, mocker, caplog):
    block_number = 20649502
    fetched_block_numbers = [block_number]

    mocker.patch.object(BlockDownloader, 'check_fetched_blocks', return_value=True)

    with caplog.at_level('DEBUG'):
        block_downloader.download_single_block(block_number, fetched_block_numbers)

    mock_api.get_block_timestamp.assert_not_called()
    mock_api.get_block_transactions.assert_not_called()
    mock_file_manager.save_to_json.assert_not_called()

    assert f"download successful" not in caplog.text


@pytest.mark.unit
def test_download_single_block_raises_exception(block_downloader, mock_api, mock_file_manager, caplog):
    block_number = 20649502
    fetched_block_numbers = []

    mock_api.get_block_timestamp.side_effect = Exception("Test exception")

    with caplog.at_level('ERROR'), pytest.raises(Exception, match="Test exception"):
        block_downloader.download_single_block(block_number, fetched_block_numbers)

    mock_api.get_block_timestamp.assert_called_once_with(0x13b161e)
    mock_api.get_block_transactions.assert_not_called()
    mock_file_manager.save_to_json.assert_not_called()

    assert f"Error while downloading block {block_number}" in caplog.text
    assert f"Error while downloading block {block_number}" in caplog.text


@pytest.mark.unit
def test_check_fetched_blocks_already_fetched(caplog):
    block_number = 20649502
    fetched_block_numbers = [20649502, 20649503]

    with caplog.at_level('DEBUG'):
        result = BlockDownloader.check_fetched_blocks(block_number, fetched_block_numbers)

    assert result is True
    assert f"Block {block_number} already fetched. Skipping..." in caplog.text


@pytest.mark.unit
def test_check_fetched_blocks_not_fetched(caplog):
    block_number = 20649502
    fetched_block_numbers = [20649503, 20649504]

    with caplog.at_level('DEBUG'):
        result = BlockDownloader.check_fetched_blocks(block_number, fetched_block_numbers)

    assert result is False
    assert f"Block {block_number} already fetched. Skipping..." not in caplog.text