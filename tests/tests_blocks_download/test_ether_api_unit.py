import pytest
import requests
import logging
from unittest.mock import patch, MagicMock
from blocks_download import EtherAPI, Utils


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


# GET_LATEST_BLOCK_NUMBER #
@pytest.mark.unit
def test_get_latest_block_number_success(caplog, api):
    mock_response = MagicMock()
    api._build_endpoint = MagicMock(return_value="mocked_endpoint")
    api._get_response = MagicMock(return_value=mock_response)
    api._parse_response = MagicMock(return_value="138EA39")
    Utils.hex_to_int = MagicMock(return_value=20507193)
    Utils.check_empty_result = MagicMock(return_value=None)

    with caplog.at_level(logging.DEBUG):
        latest_block_number = api.get_latest_block_number()

    assert latest_block_number == 20507193

    assert "Requesting latest block number from Ethereum API." in caplog.text
    assert "Latest block number retrieved: 20507193" in caplog.text

    api._build_endpoint.assert_called_once_with('proxy', 'eth_blockNumber')
    api._get_response.assert_called_once_with("mocked_endpoint")
    api._parse_response.assert_called_once_with(mock_response, "result")
    Utils.check_empty_result.assert_called_once_with("138EA39", "result for latest block number")
    Utils.hex_to_int.assert_called_once_with("138EA39")
    assert latest_block_number == 20507193


# GET_BLOCK_TIMESTAMP #
@pytest.mark.unit
def test_get_block_timestamp_success(caplog, api):
    mock_response = MagicMock()
    api._build_endpoint = MagicMock(return_value="mocked_endpoint")
    api._get_response = MagicMock(return_value=mock_response)
    api._parse_response = MagicMock(return_value={"timestamp": "0x66B9045F"})
    Utils.hex_to_int = MagicMock(return_value=1723401311)
    Utils.int_to_hex = MagicMock(return_value="0x1392e19")
    Utils.check_empty_result = MagicMock()

    with caplog.at_level(logging.DEBUG):
        block_timestamp = api.get_block_timestamp(20507193)

    assert block_timestamp == 1723401311
    assert f"Requesting block timestamp for block number: 20507193" in caplog.text
    assert "Block timestamp retrieved: 1723401311" in caplog.text

    api._get_response.assert_called_once_with("mocked_endpoint")
    api._build_endpoint.assert_called_once_with('proxy', 'eth_getBlockByNumber', tag="0x1392e19", boolean="true")
    api._parse_response.assert_called_once_with(mock_response, "result")

    Utils.check_empty_result.assert_any_call({"timestamp": "0x66B9045F"}, "result for block timestamp")
    Utils.check_empty_result.assert_any_call("0x66B9045F", "timestamp in result")

    Utils.hex_to_int.assert_called_once_with("0x66B9045F")
    Utils.int_to_hex.assert_called_once_with(20507193)


# GET_BLOCK_TRANSACTIONS #
@pytest.mark.unit
def test_get_block_transactions_success(caplog, api):
    mock_response = MagicMock()
    api._build_endpoint = MagicMock(return_value="mocked_endpoint")
    api._get_response = MagicMock(return_value=mock_response)
    transactions_list = [{"hash": "0xabc"}, {"hash": "0xdef"}]
    api._parse_response = MagicMock(return_value={"transactions": transactions_list})

    Utils.int_to_hex = MagicMock(return_value="0x1392e19")
    Utils.check_empty_result = MagicMock()
    Utils.check_type = MagicMock()

    with caplog.at_level(logging.DEBUG):
        block_transactions = api.get_block_transactions(20507193)

    assert block_transactions == transactions_list
    assert f"Requesting block transactions for block number" in caplog.text
    assert f"Number of transactions retrieved for block number 20507193: {len(transactions_list)}" in caplog.text

    api._build_endpoint.assert_called_once_with(
        'proxy', 'eth_getBlockByNumber', tag="0x1392e19", boolean="true"
    )
    api._get_response.assert_called_once_with("mocked_endpoint")
    api._parse_response.assert_called_once_with(mock_response, "result")

    Utils.check_empty_result.assert_any_call({"transactions": transactions_list}, "result for block transactions")
    Utils.check_empty_result.assert_any_call(transactions_list, "transactions in result")

    Utils.check_type.assert_called_once_with(transactions_list, list, "transactions")


# _GET_RESPONSE #
@pytest.mark.unit
def test_get_response_success(caplog, mock_requests_get, api):
    mock_response = mock_requests_get.return_value
    mock_response.status_code = 200
    mock_response.reason = "OK"
    mock_response.text = "Success"
    mock_response.raise_for_status.return_value = None

    with caplog.at_level(logging.DEBUG):
        response = api._get_response("http://dummy.url")

    mock_requests_get.assert_called_once_with("http://dummy.url", timeout=None)
    mock_response.raise_for_status.assert_called_once()

    assert response is mock_response
    assert "Request succeeded with status code: 200" in caplog.text


@pytest.mark.unit_external
def test_get_response_external(caplog, api):
    url = "https://httpbin.org/status/200"

    with caplog.at_level(logging.DEBUG):
        response = api._get_response(url)

    assert response.status_code == 200
    assert response.reason == "OK"
    assert "Request succeeded with status code: 200" in caplog.text

# _BUILD_ENDPOINT #
@pytest.mark.unit
def test_build_endpoint_no_params(api):
    endpoint = api._build_endpoint("proxy", "eth_getBlockByNumber")
    expected = "http://mock.url?module=proxy&action=eth_getBlockByNumber&apikey=mock_api_key"
    assert endpoint == expected


@pytest.mark.unit
def test_build_endpoint_with_params(api):
    params = {
        'tag': '0x1a2b',
        'boolean': 'true'
    }
    endpoint = api._build_endpoint("proxy", "eth_getBlockByNumber", params)
    expected = "http://mock.url?module=proxy&action=eth_getBlockByNumber&apikey=mock_api_key&tag=0x1a2b&boolean=true"
    assert endpoint == expected

@pytest.mark.unit
def test_build_endpoint_missing_api_url_or_key(api):
    api.config.API_URL = None
    api.config.API_KEY = None

    with pytest.raises(ValueError, match="Missing API_URL or API_KEY in configuration"):
        api._build_endpoint("proxy", "eth_getBlockByNumber")


# _PARSE_RESPONSE #
@pytest.mark.unit
def test_parse_response_found():
    fake_response = MagicMock(spec=requests.Response)
    fake_response.json.return_value = {"result": "value"}

    result = EtherAPI._parse_response(fake_response, "result")

    assert result == "value"


@pytest.mark.unit
def test_parse_response_error():
    fake_response = MagicMock(spec=requests.Response)
    fake_response.json.side_effect = ValueError("Invalid JSON response")

    with pytest.raises(ValueError, match="Invalid JSON response"):
        EtherAPI._parse_response(fake_response, "result")