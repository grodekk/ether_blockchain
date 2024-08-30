import json
import pytest
import os
import logging
from unittest.mock import mock_open, patch, call
from blocks_download import FileManager, Config  
from jsonschema import validate, ValidationError

@pytest.fixture
def config():
    return Config()

@pytest.fixture
def file_manager(config):
    return FileManager(config)

# get_file_path tests #  
def test_get_file_path(file_manager):
    filename = "test.json"
    expected_path = os.path.join(file_manager.config.BLOCKS_DATA_DIR, filename)
    assert file_manager._get_file_path(filename) == expected_path

# save_to_json tests #    
@patch("builtins.open", new_callable=mock_open)
def test_save_to_json_success(mock_open, file_manager, caplog):
    """
    Test for successful loading of data from JSON file.
    """
    data = {"key": "value"}
    filename = "test.json"

    with caplog.at_level(logging.INFO):
        file_manager.save_to_json(data, filename)
    
    written_data = "".join(call.args[0] for call in mock_open().write.call_args_list)
    expected_data = json.dumps(data, indent=4)
    assert written_data == expected_data
        
    assert "Block data saved to JSON file:" in caplog.text 
    
    with patch('blocks_download.logger', autospec=True) as mock_logger:
        file_manager.save_to_json(data, filename)

        expected_info_calls = [
            call.info(f"Block data saved to JSON file: {file_manager._get_file_path(filename)}")
        ]
        mock_logger.assert_has_calls(expected_info_calls)
        assert mock_logger.info.call_count == 1


@patch("builtins.open", new_callable=mock_open)
def test_save_to_json_empty_data(mock_open, file_manager, caplog):
    """
    Test for saving empty data to JSON file.
    """
    data = {}
    filename = "empty.json"

    with caplog.at_level(logging.ERROR):
        with pytest.raises(ValueError, match="Cannot save empty data to JSON file."):
            file_manager.save_to_json(data, filename)          
    
    assert "Cannot save empty data to JSON file." in caplog.text

    with patch('blocks_download.logger', autospec=True) as mock_logger:
        with pytest.raises(ValueError, match="Cannot save empty data to JSON file."):
            file_manager.save_to_json(data, filename)
        
            expected_error_calls = [
                call.error("Cannot save empty data to JSON file.")
            ]

            mock_logger.assert_has_calls(expected_error_calls)
            assert mock_logger.error.call_count == 1


@patch("builtins.open", new_callable=mock_open)
@patch("json.dump", side_effect=OSError("Failed to write"))
def test_save_to_json_oserror(mock_json_dump, mock_open, file_manager, caplog):
    """
    Test for handling OSError when saving data to JSON file.
    """
    data = {"key": "value"}
    filename = "test.json"
    
    with caplog.at_level(logging.ERROR):
        with pytest.raises(OSError, match="Failed to save data"):
            file_manager.save_to_json(data, filename)
        
        assert "Failed to save data" in caplog.text
    
    with patch('blocks_download.logger', autospec=True) as mock_logger:
        with pytest.raises(OSError, match="Failed to save data"):
            file_manager.save_to_json(data, filename)
        
        expected_error_calls = [
            call.error(f"Failed to save data to {file_manager._get_file_path(filename)}: Failed to write")
        ]

        mock_logger.assert_has_calls(expected_error_calls)
        assert mock_logger.error.call_count == 1


@patch("builtins.open", new_callable=mock_open, read_data=json.dumps({"key": "value"}))
def test_load_from_json_success(mock_open, file_manager, caplog):
    """
    Test for successful loading of data from JSON file.
    """
    filename = "test.json"
    expected_data = {"key": "value"}

    with caplog.at_level(logging.INFO):
        data = file_manager.load_from_json(filename)    
    
    assert data == expected_data
    
    assert "Data loaded from JSON file:" in caplog.text

    with patch('blocks_download.logger', autospec=True) as mock_logger:
        file_manager.load_from_json(filename)

        expected_info_calls = [
            call.info(f"Data loaded from JSON file: {file_manager._get_file_path(filename)}")
        ]
        mock_logger.assert_has_calls(expected_info_calls)
        assert mock_logger.info.call_count == 1

@patch("builtins.open", side_effect=FileNotFoundError("File not found"))
def test_load_from_json_file_not_found(mock_open, file_manager, caplog):
    """
    Test for handling FileNotFoundError when loading data from JSON file.
    """
    filename = "nonexistent.json"

    with caplog.at_level(logging.ERROR):
        with pytest.raises(FileNotFoundError, match="File not found."):
            file_manager.load_from_json(filename)

        assert "File not found" in caplog.text

    with patch('blocks_download.logger', autospec=True) as mock_logger:
        with pytest.raises(FileNotFoundError, match="File not found."):
            file_manager.load_from_json(filename)
        
        expected_error_calls = [
            call.error(f"File not found: {file_manager._get_file_path(filename)}. Raising an exception.")
        ]

        mock_logger.assert_has_calls(expected_error_calls)
        assert mock_logger.error.call_count == 1


@patch("builtins.open", new_callable=mock_open)
@patch("json.load", side_effect=json.JSONDecodeError("Expecting value", "", 0))
def test_load_from_json_json_decode_error(mock_json_load, mock_open, file_manager, caplog):
    """
    Test for handling JSONDecodeError when loading data from JSON file.
    """
    filename = "invalid.json"

    with caplog.at_level(logging.ERROR):
        with pytest.raises(ValueError, match="Invalid JSON format."):
            file_manager.load_from_json(filename)
        
        # Sprawdzenie, czy komunikat o błędzie pojawił się w logach
        assert "Error decoding JSON from file" in caplog.text

    with patch('blocks_download.logger', autospec=True) as mock_logger:
        with pytest.raises(ValueError, match="Invalid JSON format."):
            file_manager.load_from_json(filename)
        
        expected_error_calls = [
            call.error(f"Error decoding JSON from file {file_manager._get_file_path(filename)}: Expecting value: line 1 column 1 (char 0)")
        ]

        mock_logger.assert_has_calls(expected_error_calls)
        assert mock_logger.error.call_count == 1


# json file structure tests #
block_transaction_data = {
    "block_number": 20542977,
    "timestamp": 1723832831,
    "transactions": [
        {
            "blockHash": "0x99a05b3bba35bd4a4d2457818c1442fbdfd7d2cfdf7069c28a91830e5528f95d",
            "blockNumber": "0x1397601",
            "from": "0x56090aa3b9d84e485fade309002c12b17f1669ee",
            "gas": "0x5208",
            "gasPrice": "0x5a936029d",
            "hash": "0xaa6f22324f68598dc77fc80ed75ed65cc5faf290f7078d14f44454417198c592",
            "input": "0x",
            "nonce": "0x28",
            "to": "0xf55cb5784fd0eafc2b0d1fee03ee178667b55987",
            "transactionIndex": "0x0",
            "value": "0x2386f26fc10000",
            "type": "0x0",
            "chainId": "0x1",
            "v": "0x25",
            "r": "0xe8202f0bd190f314546379cfda0b95a8692c30569199c164db4861fec21c4bfb",
            "s": "0x3ee1d94fa34cde5260d51adf953a3e05adb3e1220d5cf56241c6aeaf91aa13ae"
        }
    ]
}

schema = {
    "type": "object",
    "properties": {
        "block_number": {"type": "integer"},
        "timestamp": {"type": "integer"},
        "transactions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "blockHash": {"type": "string"},
                    "blockNumber": {"type": "string"},
                    "from": {"type": "string"},
                    "gas": {"type": "string"},
                    "gasPrice": {"type": "string"},
                    "hash": {"type": "string"},
                    "input": {"type": "string"},
                    "nonce": {"type": "string"},
                    "to": {"type": "string"},
                    "transactionIndex": {"type": "string"},
                    "value": {"type": "string"},
                    "type": {"type": "string"},
                    "chainId": {"type": "string"},
                    "v": {"type": "string"},
                    "r": {"type": "string"},
                    "s": {"type": "string"}
                },
                "required": ["blockNumber", "from", "to", "value", "gasPrice"]
            }
        }
    },
    "required": ["block_number", "timestamp", "transactions"]
}

@patch("builtins.open", new_callable=mock_open, read_data=json.dumps(block_transaction_data))
def test_load_core_data_from_json_structure(mock_open, file_manager):
    filename = "structure.json"
    result = file_manager.load_from_json(filename)    
  
    assert "block_number" in result
    assert isinstance(result["block_number"], int)    
    assert "timestamp" in result
    assert isinstance(result["timestamp"], int)        
    assert "transactions" in result
    assert isinstance(result["transactions"], list)
    assert len(result["transactions"]) > 0    

    for transaction in result["transactions"]:
        assert "gasPrice" in transaction
        assert "from" in transaction
        assert "to" in transaction
        assert "value" in transaction

@patch("builtins.open", new_callable=mock_open, read_data=json.dumps(block_transaction_data))
def test_load_from_json_schema(mock_open, file_manager):
    filename = "large_file.json"
    result = file_manager.load_from_json(filename)
    
    try:
        validate(instance=result, schema=schema)
    except ValidationError as e:
        pytest.fail(f"JSON schema validation error: {e.message}")