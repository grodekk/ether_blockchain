import pytest
from unittest.mock import patch, MagicMock
from blocks_download import EtherAPI, Utils
import requests
import logging
from error_handler import CustomProcessingError

@pytest.fixture
def real_api():
    from config import Config
    config = Config()
    return EtherAPI(config_instance=config)

@pytest.fixture
def missing_url_key_api():
    mock_config = MagicMock()
    mock_config.API_URL = None
    mock_config.API_KEY = None
    return EtherAPI(config_instance=mock_config)

@pytest.fixture
def mock_config():
    config = MagicMock()
    config.API_URL = "http://mock.url"
    config.API_KEY = "mock_api_key"
    return config

@pytest.fixture
def api(mock_config):
    return EtherAPI(config_instance=mock_config)

@pytest.fixture
def mock_requests_get():
    with patch('blocks_download.requests.get') as mock:
        yield mock


@pytest.mark.integration
def test_get_latest_block_number_error(caplog, api):
    api._get_response = MagicMock()
    Utils.check_empty_result = MagicMock(side_effect=CustomProcessingError(ValueError("Empty result")))

    with caplog.at_level(logging.ERROR):
        with pytest.raises(CustomProcessingError, match="Empty result"):
            api.get_latest_block_number()

    assert "Latest block number retrieved:" not in caplog.text


@pytest.mark.integration
def test_get_block_timestamp_error(caplog, api):
    api._get_response = MagicMock()
    Utils.check_empty_result = MagicMock(side_effect=[None, CustomProcessingError(ValueError("Empty timestamp"))])

    with caplog.at_level(logging.ERROR):
        with pytest.raises(CustomProcessingError, match="Empty timestamp"):
            api.get_block_timestamp(123)

    assert "Latest block number retrieved:" not in caplog.text


@pytest.mark.integration
def test_get_block_transactions_error(caplog, api):
    api._get_response = MagicMock()
    Utils.check_empty_result = MagicMock(side_effect=[None, CustomProcessingError(ValueError("Empty timestamp"))])

    with caplog.at_level(logging.ERROR):
        with pytest.raises(CustomProcessingError, match="Empty timestamp"):
            api.get_block_timestamp(123)

    assert "Latest block number retrieved:" not in caplog.text


# _GET_RESPONSE #
@pytest.mark.integration
def test_get_response_connection_error(caplog, mock_requests_get, api):
    mock_requests_get.side_effect = requests.ConnectionError("Connection error")

    with caplog.at_level(logging.ERROR):
        with pytest.raises(CustomProcessingError):
            api._get_response("http://dummy.url")

    mock_requests_get.assert_called_once_with("http://dummy.url", timeout=None)
    assert "Connection error" in caplog.text


# _BUILD_ENDPOINT #
@pytest.mark.integration
def test_build_endpoint_missing_config(caplog, api):
    api.config = MagicMock()
    api.config.API_URL = None
    api.config.API_KEY = None

    with caplog.at_level(logging.ERROR):
        with pytest.raises(CustomProcessingError):
            api._build_endpoint('proxy', 'eth_getBlockByNumber')

    assert "ValueError" in caplog.text
    assert "Missing API_URL or API_KEY in configuration" in caplog.text


# _PARSE_RESPONSE #
@pytest.mark.integration
def test_parse_response_invalid_json(caplog, api):
    mock_response = MagicMock(spec=requests.Response)
    mock_response.json.side_effect = ValueError("Invalid JSON response")

    with caplog.at_level(logging.ERROR):
        with pytest.raises(CustomProcessingError, match="Invalid JSON response"):
            api._parse_response(mock_response, 'result')

    assert "ValueError" in caplog.text
    assert "Invalid JSON response" in caplog.text


# EtherApi_CLASS #
@pytest.mark.class_integration
def test_integration_success(real_api, caplog):
    block_number = real_api.get_latest_block_number()
    assert block_number > 0

    block_timestamp = real_api.get_block_timestamp(block_number)
    assert block_timestamp > 0

    transactions = real_api.get_block_transactions(block_number)
    assert isinstance(transactions, list)
    assert len(transactions) >= 0

    expected_fragments = [
        "Requesting latest block number from Ethereum API.",
        "Sending GET request",
        "Parsing response, looking for key",
        "Latest block number retrieved",
        "Requesting block timestamp for block number",
        "Block timestamp retrieved",
        "Requesting block transactions for block number",
        "Number of transactions retrieved for block number"
    ]

    for fragment in expected_fragments:
        assert fragment in caplog.text, f"Expected log fragment '{fragment}' not found in logs."


@pytest.mark.class_integration
def test_integration_invalid_api_key(missing_url_key_api, caplog):
    with pytest.raises(CustomProcessingError):
        missing_url_key_api.get_latest_block_number()

    assert "ValueError" in caplog.text
    assert "Missing API_URL or API_KEY in configuration" in caplog.text
