import pytest
from unittest.mock import patch
from blocks_download import EtherAPI
import requests

class DummyConfig:
    API_URL = "http://dummy.url"
    API_KEY = "dummy_key"

@pytest.fixture
def mock_requests_get():
    with patch('blocks_download.requests.get') as mock:
        yield mock

@pytest.fixture
def api(mock_requests_get):
        config = DummyConfig()
        return EtherAPI(config=config)

class TestEtherAPI:
    # _get_response tests #        
    def test_get_response_http_success(self, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 200
        response = api._get_response("http://dummy.url")
        assert response.status_code == 200
    
    def test_get_response_http_error(self, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 404
        with pytest.raises(ConnectionError, match="HTTP error occurred: 404"):
            api._get_response("http://dummy.url")
    
    def test_get_response_timeout(self, mock_requests_get, api):
        mock_requests_get.side_effect = requests.Timeout("The request timed out")
        with pytest.raises(ConnectionError, match="Request failed: The request timed out"):
            api._get_response("http://dummy.url")
    
    def test_get_response_request_exception(self, mock_requests_get, api):
        mock_requests_get.side_effect = requests.RequestException("General error")
        with pytest.raises(ConnectionError, match="Request failed: General error"):
            api._get_response("http://dummy.url")

    # get_latest_block_number tests #    
    def test_get_latest_block_number_http_success(self, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"result": "138EA39"}
        latest_block_number = api.get_latest_block_number()
        assert latest_block_number == 20507193   
    
    def test_get_latest_block_number_http_error(self, mock_requests_get, api):        
        mock_requests_get.return_value.status_code = 404            
        with pytest.raises(ConnectionError, match="HTTP error occurred: 404"):
            api.get_latest_block_number()            

    def test_get_latest_block_number_empty_result(self, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"result": None}
        with pytest.raises(ValueError, match="Empty result for block number."):
            api.get_latest_block_number()  

    def test_get_latest_block_number_invalid_format(self, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"result": "0xxyz"}  # Nieprawidłowy format hex
        with pytest.raises(ValueError, match="Invalid block number format: 0xxyz"):
            api.get_latest_block_number()


    # get_block_timestamp tests #    
    def test_get_block_timestamp_http_success(self, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"result": {"timestamp": "66B9045F"}}
        timestamp = api.get_block_timestamp("138EA39")        
        assert timestamp == 1723401311
    
    def test_get_block_timestamp_http_error(self, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 404
        with pytest.raises(ConnectionError, match="HTTP error occurred: 404"):
            api.get_block_timestamp("138EA39")

    def test_get_block_timestamp_empty_result(self, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"result": {}}
        with pytest.raises(ValueError, match="Empty result for block timestamp."):
            api.get_block_timestamp("138EA39")
    
    def test_get_block_timestamp_empty_timestamp(self, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"result": {"timestamp": None}}
        with pytest.raises(ValueError, match="Empty timestamp in result."):
            api.get_block_timestamp("138EA39")   

    def test_get_block_timestamp_invalid_format(self, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"result": {"timestamp": "xyz"}}
        with pytest.raises(ValueError, match="Invalid timestamp format: xyz"):
            api.get_block_timestamp("138EA39")


    # get_block_transactions tests #    
    def test_get_block_transactions_http_success(self, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"result": {"transactions": ["tx1", "tx2"]}}
        transactions = api.get_block_transactions("138EA39")
        assert transactions == ["tx1", "tx2"]
    
    def test_get_block_transactions_http_error(self, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 404
        with pytest.raises(ConnectionError, match="HTTP error occurred: 404"):
            api.get_block_transactions("138EA39")

    def test_get_block_transactions_empty_result(self, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"result": {}}
        with pytest.raises(ValueError, match="Empty result for block transactions."):
            api.get_block_transactions("138EA39")

    def test_get_block_transactions_invalid_format(self, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"result": {"transactions": "not_a_list"}}
        with pytest.raises(ValueError, match="Invalid transactions format: not_a_list"):
            api.get_block_transactions("138EA39")
    
    def test_get_block_transactions_empty_transactions(self, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"result": {"transactions": []}}
        with pytest.raises(ValueError, match="Empty transactions for transactions result."):
            api.get_block_transactions("138EA39")    