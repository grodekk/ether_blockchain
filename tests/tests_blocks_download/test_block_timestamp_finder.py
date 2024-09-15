import logging
import pytest
from unittest.mock import patch, MagicMock, call
from blocks_download import BlockTimestampFinder, EtherAPI


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

@pytest.fixture
def block_timestamp_finder(api):
    return BlockTimestampFinder(api=api)


class TestBlockTimestampFinder:

    # get_timestamp_of_FIRST_block_on_target_date tests#
    def test_get_timestamp_of_first_block_on_target_date_success(self, caplog, block_timestamp_finder):
        target_date = "2024-08-01"
        target_timestamp = 1693440000
        latest_block_number = 20649806

        block_timestamps = {
            "0x9d8ba7": 1592953660,
            "0xec517b": 1662512503,
            "0x113b465": 1693896383,
            "0x12765da": 1709523887,
            "0x1313e94": 1717335707,
            "0x1362af1": 1721230235,
            "0x138a120": 1723175627,
            "0x1376608": 1722203015,
            "0x1380394": 1722689699,
            "0x137b4ce": 1722446399,
            "0x137dc31": 1722567983,
            "0x137c87f": 1722507155,
            "0x137bea6": 1722476771,
            "0x137b9ba": 1722461603,
            "0x137bc30": 1722469187,
            "0x137bd6b": 1722472979,
            "0x137bccd": 1722471083,
            "0x137bc7e": 1722470123,
            "0x137bca5": 1722470603,
            "0x137bc91": 1722470363,
            "0x137bc9b": 1722470483,
            "0x137bc96": 1722470423,
            "0x137bc93": 1722470387,
            "0x137bc94": 1722470399,
            "0x137bc95": 1722470411,            
        }
        
        mock_get_latest_block_number = MagicMock(return_value=latest_block_number)
        mock_get_block_timestamp = MagicMock(side_effect=lambda block_number: block_timestamps.get(block_number, 0))

        with patch.object(block_timestamp_finder.api, 'get_latest_block_number', mock_get_latest_block_number), \
            patch.object(block_timestamp_finder.api, 'get_block_timestamp', mock_get_block_timestamp):
            
            with caplog.at_level(logging.DEBUG):
                first_block_number = block_timestamp_finder.get_timestamp_of_first_block_on_target_date(target_date)
           
            assert any(record.levelname == "INFO" and "Starting search for the first block on 2024-08-01" in record.message for record in caplog.records)          
            assert any(record.levelname == "DEBUG" and "Checking block number:" in record.message for record in caplog.records)           
            assert any(record.levelname == "INFO" and f"First block on {target_date} is {first_block_number}" in record.message for record in caplog.records)            
            assert any(record.levelname == "DEBUG" and f"Final check for block number: {first_block_number}" in record.message for record in caplog.records)

            
            mock_get_latest_block_number.assert_called_once()
            mock_get_block_timestamp.assert_called()
            
            expected_first_block_number = 20429973
            assert first_block_number == expected_first_block_number         


    def test_invalid_date_format(self, caplog, block_timestamp_finder):
        invalid_date = "2024-08-32"  
        
        with caplog.at_level(logging.ERROR):
            with pytest.raises(ValueError, match="Date must be in the format YYYY-MM-DD"):
                block_timestamp_finder.get_timestamp_of_first_block_on_target_date(invalid_date)
        
        assert "Invalid target date format: 2024-08-32" in caplog.text


    def test_error_initializing_search(self, caplog, block_timestamp_finder):
        target_date = "2024-08-01"
                
        with patch.object(block_timestamp_finder.api, 'get_latest_block_number', side_effect=Exception("API error")):
            with caplog.at_level(logging.ERROR):
                with pytest.raises(RuntimeError, match="Failed to initialize the search for the first block timestamp"):
                    block_timestamp_finder.get_timestamp_of_first_block_on_target_date(target_date)
        
        assert "Error initializing search: API error" in caplog.text


    def test_error_fetching_block_timestamp(self, caplog, block_timestamp_finder):
        target_date = "2024-08-01"
        latest_block_number = 20649806        
        
        with patch.object(block_timestamp_finder.api, 'get_latest_block_number', return_value=latest_block_number), \
            patch.object(block_timestamp_finder.api, 'get_block_timestamp', side_effect=Exception("API timestamp error")):
            with caplog.at_level(logging.ERROR):
                with pytest.raises(RuntimeError, match="Failed to fetch block timestamp for block"):
                    block_timestamp_finder.get_timestamp_of_first_block_on_target_date(target_date)
        
        assert "Error fetching block timestamp for block" in caplog.text
    

    def test_error_fetching_final_block_timestamp(self, caplog, block_timestamp_finder):
        target_date = "2024-08-01"
        latest_block_number = 20649806         
        hex_latest_block_number = hex(latest_block_number) 
        expected_first_block_number = 20429973
        
        def mock_get_block_timestamp(block_number):
            if block_number == hex_latest_block_number:
                raise Exception("Final block error")
            return 1693439999
        
        with patch.object(block_timestamp_finder.api, 'get_latest_block_number', return_value=latest_block_number), \
            patch.object(block_timestamp_finder.api, 'get_block_timestamp', side_effect=mock_get_block_timestamp):
            
            with caplog.at_level(logging.ERROR):                
                with pytest.raises(RuntimeError, match=f"Failed to fetch final block timestamp for block {latest_block_number}"):
                    block_timestamp_finder.get_timestamp_of_first_block_on_target_date(target_date)
        
        assert f"Error fetching final block timestamp for block {latest_block_number}" in caplog.text
        assert "Final block error" in caplog.text


    # get_timestamp_of_LAST_block_on_target_date tests#
    def test_get_timestamp_of_last_block_on_target_date_success(self, caplog, block_timestamp_finder):
        target_date = "2024-08-01"
        target_timestamp = 1693439999  
        latest_block_number = 20715574
                
        block_timestamps = {
            "0x9e0c1b": 1593393842,
            "0xed1229": 1663213721,
            "0x1149530": 1694592875,
            "0x12856b3": 1710268859,
            "0x1323775": 1718104427,
            "0x13727d6": 1722011003,
            "0x139a006": 1723962671,
            "0x13863ee": 1722986927,
            "0x137c5e2": 1722499079,
            "0x13814e8": 1722743147,
            "0x137ed65": 1722621143,
            "0x137d9a3": 1722560099,
            "0x137cfc2": 1722529583,
            "0x137d4b2": 1722544847,
            "0x137d72a": 1722552455,
            "0x137d866": 1722556283,
            "0x137d904": 1722558191,
            "0x137d8b5": 1722557231,
            "0x137d88d": 1722556751,
            "0x137d8a1": 1722556991,
            "0x137d897": 1722556871,
            "0x137d892": 1722556811,
            "0x137d88f": 1722556775,
            "0x137d890": 1722556787,
            "0x137d891": 1722556799,
        }
        mock_get_latest_block_number = MagicMock(return_value=latest_block_number)
        mock_get_block_timestamp = MagicMock(side_effect=lambda block_number: block_timestamps.get(block_number, 0))

        with patch.object(block_timestamp_finder.api, 'get_latest_block_number', mock_get_latest_block_number), \
            patch.object(block_timestamp_finder.api, 'get_block_timestamp', mock_get_block_timestamp):      

            with caplog.at_level(logging.DEBUG):
                last_block_number = block_timestamp_finder.get_timestamp_of_last_block_on_target_date(target_date)

            assert any(record.levelname == "INFO" and "Starting search for the last block on 2024-08-01" in record.message for record in caplog.records)
            assert any(record.levelname == "DEBUG" and "Checking block number:" in record.message for record in caplog.records)
            assert any(record.levelname == "INFO" and f"Last block on {target_date} is {last_block_number}" in record.message for record in caplog.records)
            assert any(record.levelname == "DEBUG" and f"Final check for block number: {last_block_number}" in record.message for record in caplog.records)

            mock_get_latest_block_number.assert_called_once()
            mock_get_block_timestamp.assert_called()

            expected_last_block_number = 20437137
            assert last_block_number == expected_last_block_number


    def test_invalid_date_format_for_last(self, caplog, block_timestamp_finder):
        invalid_date = "2024-08-32"  
        
        with caplog.at_level(logging.ERROR):
            with pytest.raises(ValueError, match="Date must be in the format YYYY-MM-DD"):
                block_timestamp_finder.get_timestamp_of_last_block_on_target_date(invalid_date)
        
        assert "Invalid target date format: 2024-08-32" in caplog.text
        

    def test_error_initializing_search_for_last(self, caplog, block_timestamp_finder):
        target_date = "2024-08-01"
        
        with patch.object(block_timestamp_finder.api, 'get_latest_block_number', side_effect=Exception("API error")):
            with caplog.at_level(logging.ERROR):
                with pytest.raises(RuntimeError, match="Failed to initialize the search for the last block timestamp"):
                    block_timestamp_finder.get_timestamp_of_last_block_on_target_date(target_date)
        
        assert "Error initializing search: API error" in caplog.text


    def test_error_fetching_block_timestamp_for_last(self, caplog, block_timestamp_finder):
        target_date = "2024-08-01"
        latest_block_number = 20649806  
        
        with patch.object(block_timestamp_finder.api, 'get_latest_block_number', return_value=latest_block_number), \
            patch.object(block_timestamp_finder.api, 'get_block_timestamp', side_effect=Exception("API timestamp error")):
            with caplog.at_level(logging.ERROR):
                with pytest.raises(RuntimeError, match="Failed to fetch block timestamp for block"):
                    block_timestamp_finder.get_timestamp_of_last_block_on_target_date(target_date)
        
        assert "Error fetching block timestamp for block" in caplog.text


    def test_error_fetching_final_block_timestamp_for_last(self, caplog, block_timestamp_finder):
        target_date = "2024-08-01"
        latest_block_number = 20649806  
        hex_latest_block_number = hex(latest_block_number)
        expected_last_block_number = 20429973
        
        def mock_get_block_timestamp(block_number):
            if block_number == hex_latest_block_number:
                raise Exception("Final block error")
            return 1693439999
        
        with patch.object(block_timestamp_finder.api, 'get_latest_block_number', return_value=latest_block_number), \
            patch.object(block_timestamp_finder.api, 'get_block_timestamp', side_effect=mock_get_block_timestamp):
            
            with caplog.at_level(logging.ERROR):                
                with pytest.raises(RuntimeError, match=f"Failed to fetch final block timestamp for block {latest_block_number}"):
                    block_timestamp_finder.get_timestamp_of_last_block_on_target_date(target_date)
        
        assert f"Error fetching final block timestamp for block {latest_block_number}" in caplog.text
        assert "Final block error" in caplog.text


    # get_timestamp_of_LAST_block_on_target_date tests#
    def test_validate_date(self, block_timestamp_finder):
        assert block_timestamp_finder._validate_date("2024-08-01") == True
        assert block_timestamp_finder._validate_date("2024-08-32") == False
        assert block_timestamp_finder._validate_date("not-a-date") == False