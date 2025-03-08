import pytest
from unittest.mock import patch, call, MagicMock
from blocks_download import EtherAPI
import requests
import logging

from error_handler import CustomProcessingError


class DummyConfig:
    API_URL = "http://dummy.url"
    API_KEY = "dummy_key"

@pytest.fixture
def api(mock_requests_get):
        config = DummyConfig()
        return EtherAPI(config=config)

@pytest.fixture
def mock_requests_get():
    with patch('blocks_download.requests.get') as mock:
        yield mock


class TestEtherAPI:

    # _get_response tests #
    def test_get_response_success(self, caplog, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.reason = "OK"
        mock_requests_get.return_value.text = "Success"

        with caplog.at_level(logging.INFO):
            response = api._get_response("http://dummy.url")

        assert response.status_code == 200
        assert response.reason == "OK"
        assert response.text == "Success"
        assert "Request succeeded with status code: 200" in caplog.text


    def test_get_response_http_error(self, caplog, mock_requests_get, api):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.reason = "NOT FOUND"
        mock_response.request.url = "https://httpbin.org/status/404"
        mock_requests_get.return_value.raise_for_status.side_effect = requests.HTTPError("404 Not Found",
                                                                                        response=mock_response)

        with caplog.at_level(logging.ERROR):
            with pytest.raises(CustomProcessingError, match="404 Not Found"):
                api._get_response("http://dummy.url")

        assert 'HTTP error' in caplog.text
        assert 'status code 404' in caplog.text
        assert 'reason NOT FOUND' in caplog.text
        assert 'URL https://httpbin.org/status/404' in caplog.text


    def test_get_response_connection_error(self, caplog, mock_requests_get, api):
        mock_requests_get.side_effect = requests.ConnectionError("Failed to establish a new connection")

        with caplog.at_level(logging.ERROR):
            with pytest.raises(CustomProcessingError, match="Failed to establish a new connection"):
                api._get_response("http://dummy.url")

        assert 'Connection error' in caplog.text
        assert 'Failed to establish a new connection' in caplog.text
        assert 'EtherAPI._get_response' in caplog.text


    def test_get_response_timeout(self, caplog, mock_requests_get, api):
        mock_requests_get.return_value.raise_for_status.side_effect = requests.Timeout("The request timed out")

        with caplog.at_level(logging.ERROR):
            with pytest.raises(CustomProcessingError, match="The request timed out"):
                api._get_response("http://dummy.url")

        assert 'Timeout error' in caplog.text
        assert 'The request timed out' in caplog.text
        assert 'EtherAPI._get_response' in caplog.text


    def test_get_response_too_many_redirects(self, caplog, mock_requests_get, api):
        mock_requests_get.return_value.raise_for_status.side_effect = requests.TooManyRedirects("redirectsError")

        with caplog.at_level(logging.ERROR):
            with pytest.raises(CustomProcessingError, match="redirectsError"):
                api._get_response("http://dummy.url")

        assert 'redirectsError' in caplog.text
        assert 'Too many redirects error' in caplog.text
        assert 'EtherAPI._get_response' in caplog.text


    def test_get_response_request_exception(self, caplog, mock_requests_get, api):
        mock_requests_get.return_value.raise_for_status.side_effect = requests.RequestException("An error occurred")

        with caplog.at_level(logging.ERROR):
            with pytest.raises(CustomProcessingError, match="An error occurred"):
                api._get_response("http://dummy.url")

        assert 'General request exception' in caplog.text
        assert 'An error occurred' in caplog.text
        assert 'EtherAPI._get_response' in caplog.text


    # get_latest_block_number tests #    
    def test_get_latest_block_number_success(self, caplog, mock_requests_get, api):        
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"result": "138EA39"}
        
        with caplog.at_level(logging.DEBUG):
            latest_block_number = api.get_latest_block_number()
        
        assert latest_block_number == 20507193        
        assert "Received response with status code: 200" in caplog.text
        assert "Latest block number retrieved: 20507193" in caplog.text
        
        with patch('blocks_download.logger', autospec=True) as mock_logger:
            latest_block_number = api.get_latest_block_number()

            expected_calls = [
                call.info('Received response with status code: 200'),                        
                call.info('Latest block number retrieved: 20507193')
            ]
            
            mock_logger.assert_has_calls(expected_calls)
            assert mock_logger.info.call_count == 2
            


    def test_get_latest_block_number_empty_result(self, caplog, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"result": None}        
        
        with caplog.at_level(logging.ERROR):
            with pytest.raises(ValueError, match="Empty result for block number."):
                api.get_latest_block_number()
                
        assert "Empty result for block number in API response." in caplog.text        
        
        with patch('blocks_download.logger', autospec=True) as mock_logger:
            with pytest.raises(ValueError, match="Empty result for block number."):
                api.get_latest_block_number()

            expected_calls = [
                call('Empty result for block number in API response.')  
            ]
            
            mock_logger.error.assert_has_calls(expected_calls)
            assert mock_logger.error.call_count == 1


    def test_get_latest_block_number_invalid_format(self, caplog, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"result": "0xxyz"}
        
        with caplog.at_level(logging.ERROR):
            with pytest.raises(ValueError, match="Invalid block number format: 0xxyz"):
                api.get_latest_block_number()
        
        assert "Invalid block number format: 0xxyz" in caplog.text
        
        with patch('blocks_download.logger', autospec=True) as mock_logger:
            with pytest.raises(ValueError, match="Invalid block number format: 0xxyz"):
                api.get_latest_block_number()

            expected_calls = [
                call('Invalid block number format: 0xxyz')
            ]
            
            mock_logger.error.assert_has_calls(expected_calls)
            assert mock_logger.error.call_count == 1
            

    def test_get_latest_block_number_json_parsing_error(self, caplog, mock_requests_get, api):        
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.side_effect = ValueError("Invalid JSON")

        with caplog.at_level(logging.ERROR):
            with pytest.raises(ValueError, match="Failed to parse JSON response."):
                api.get_latest_block_number()
                
        assert "Failed to parse JSON response: Invalid JSON" in caplog.text
                
        with patch('blocks_download.logger', autospec=True) as mock_logger:
            with pytest.raises(ValueError, match="Failed to parse JSON response."):
                api.get_latest_block_number()
            
            expected_calls = [
                call("Failed to parse JSON response: Invalid JSON")
            ]
            
            mock_logger.error.assert_has_calls(expected_calls)
            assert mock_logger.error.call_count == 1


    # get_block_timestamp tests #    
    def test_get_block_timestamp_success(self, caplog, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"result": {"timestamp": "66B9045F"}}
        
        with caplog.at_level(logging.INFO):
            timestamp = api.get_block_timestamp("138EA39")
            
            assert timestamp == 1723401311
            
            assert "Requesting block timestamp for block number: 138EA39" in caplog.text
            assert 'Received response with status code: 200' in caplog.text            
            assert "Block timestamp retrieved: 1723401311" in caplog.text
        
        with patch('blocks_download.logger', autospec=True) as mock_logger:
            timestamp = api.get_block_timestamp("138EA39")
            
            expected_calls = [
                call('Requesting block timestamp for block number: 138EA39'),
                call('Received response with status code: 200'),                
                call('Block timestamp retrieved: 1723401311')
            ]
            
            mock_logger.info.assert_has_calls(expected_calls)
            assert mock_logger.info.call_count == 3


    def test_get_block_timestamp_empty_result(self, caplog, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"result": {}}
        
        with caplog.at_level(logging.ERROR):
            with pytest.raises(ValueError, match="Empty result for block timestamp."):
                api.get_block_timestamp("138EA39")
                    
        assert "Empty result for block timestamp in API response." in caplog.text
       
        with patch('blocks_download.logger', autospec=True) as mock_logger:
            with pytest.raises(ValueError, match="Empty result for block timestamp."):
                api.get_block_timestamp("138EA39")

            expected_calls = [
                call('Empty result for block timestamp in API response.')
            ]

            mock_logger.error.assert_has_calls(expected_calls)
            assert mock_logger.error.call_count == 1


    def test_get_block_timestamp_empty_timestamp(self, caplog, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"result": {"timestamp": None}}

        with caplog.at_level(logging.ERROR):
            with pytest.raises(ValueError, match="Empty timestamp in result."):
                api.get_block_timestamp("138EA39")

        assert "Empty timestamp in result." in caplog.text

        with patch('blocks_download.logger', autospec=True) as mock_logger:
            with pytest.raises(ValueError, match="Empty timestamp in result."):
                api.get_block_timestamp("138EA39")

            expected_calls = [
                call('Empty timestamp in result.')
            ]

            mock_logger.error.assert_has_calls(expected_calls)
            assert mock_logger.error.call_count == 1


    def test_get_block_timestamp_invalid_format(self, caplog, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"result": {"timestamp": "xyz"}}

        with caplog.at_level(logging.ERROR):
            with pytest.raises(ValueError, match="Invalid timestamp format: xyz"):
                api.get_block_timestamp("138EA39")
                
        assert "Invalid timestamp format: xyz" in caplog.text
        
        with patch('blocks_download.logger', autospec=True) as mock_logger:
            with pytest.raises(ValueError, match="Invalid timestamp format: xyz"):
                api.get_block_timestamp("138EA39")

            expected_calls = [
                call('Invalid timestamp format: xyz')
            ]

            mock_logger.error.assert_has_calls(expected_calls)
            assert mock_logger.error.call_count == 1


    def test_get_block_timestamp_json_parsing_error(self, caplog, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.side_effect = ValueError("Invalid JSON")
        
        with caplog.at_level(logging.ERROR):
            with pytest.raises(ValueError, match="Failed to parse JSON response."):
                api.get_block_timestamp("138EA39")
                
        assert "Failed to parse JSON response: Invalid JSON" in caplog.text
        
        with patch('blocks_download.logger', autospec=True) as mock_logger:
            with pytest.raises(ValueError, match="Failed to parse JSON response."):
                api.get_block_timestamp("138EA39")

            expected_calls = [
                call("Failed to parse JSON response: Invalid JSON")
            ]

            mock_logger.error.assert_has_calls(expected_calls)
            assert mock_logger.error.call_count == 1
   

    # get_block_transactions tests #    
    def test_get_block_transactions_success(self, caplog, mock_requests_get, api):        
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"result": {"transactions": ["tx1", "tx2"]}}
        
        with caplog.at_level(logging.DEBUG):
            api.get_block_transactions("138EA39")
            
        assert "Requesting block transactions for block number: 138EA39" in caplog.text
        assert 'Received response with status code: 200' in caplog.text      
        assert "API response data: {'result': {'transactions': ['tx1', 'tx2']}}"
        assert "Number of transactions retrieved for block number 138EA39: 2" in caplog.text
        
        with patch('blocks_download.logger', autospec=True) as mock_logger:
            transactions = api.get_block_transactions("138EA39")
            
            expected_calls = [
                call.info('Requesting block transactions for block number: 138EA39'),
                call.info('Received response with status code: 200'),                            
                call.info('Number of transactions retrieved for block number 138EA39: 2')
            ]
            
            mock_logger.assert_has_calls(expected_calls)
            assert mock_logger.info.call_count == 3            
            

    def test_get_block_transactions_empty_result(self, caplog, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"result": {}}
        
        with caplog.at_level(logging.ERROR):
            with pytest.raises(ValueError, match="Empty result for block transactions."):
                api.get_block_transactions("138EA39")
                
        assert "Empty result for block transactions in API response." in caplog.text
        
        with patch('blocks_download.logger', autospec=True) as mock_logger:
            with pytest.raises(ValueError, match="Empty result for block transactions."):
                api.get_block_transactions("138EA39")

            expected_calls = [
                call("Empty result for block transactions in API response.")
            ]

            mock_logger.error.assert_has_calls(expected_calls)
            assert mock_logger.error.call_count == 1


    def test_get_block_transactions_invalid_format(self, caplog, mock_requests_get, api):
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"result": {"transactions": "not_a_list"}}
        
        with caplog.at_level(logging.ERROR):
            with pytest.raises(ValueError, match="Invalid transactions format: Expected list, got str."):
                api.get_block_transactions("138EA39")
                
        assert "Invalid transactions format: Expected list, got str." in caplog.text
        
        with patch('blocks_download.logger', autospec=True) as mock_logger:
            with pytest.raises(ValueError, match="Invalid transactions format: Expected list, got str."):
                api.get_block_transactions("138EA39")

            expected_calls = [
                call("Invalid transactions format: Expected list, got str.")
            ]

            mock_logger.error.assert_has_calls(expected_calls)
            assert mock_logger.error.call_count == 1

    
    def test_get_block_transactions_json_parsing_error(self, mock_requests_get, api, caplog):        
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.side_effect = ValueError("Invalid JSON")

        with caplog.at_level(logging.ERROR):
            with pytest.raises(ValueError, match="Failed to parse JSON response."):
                api.get_block_transactions("138EA39")
        
        assert "Failed to parse JSON response: Invalid JSON" in caplog.text

        with patch('blocks_download.logger', autospec=True) as mock_logger:
            with pytest.raises(ValueError, match="Failed to parse JSON response."):
                api.get_block_transactions("138EA39")

            expected_calls = [
                call.error("Failed to parse JSON response: Invalid JSON")
            ]            
            
            mock_logger.assert_has_calls(expected_calls)
            assert mock_logger.error.call_count == 1

    def test_get_block_transactions_empty_transactions(self, mock_requests_get, api, caplog):        
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {"result": {"transactions": []}}

        with caplog.at_level(logging.ERROR):
            with pytest.raises(ValueError, match="Empty transactions for transactions result."):
                api.get_block_transactions("138EA39")
        
        assert "Empty transactions for transactions result." in caplog.text
        
        with patch('blocks_download.logger', autospec=True) as mock_logger:            
            with pytest.raises(ValueError, match="Empty transactions for transactions result."):
                api.get_block_transactions("138EA39")

            expected_calls = [
                call("Empty transactions for transactions result.")
            ]

            mock_logger.error.assert_has_calls(expected_calls)
            assert mock_logger.error.call_count == 1