import json
import pytest
import os
import logging
from unittest.mock import mock_open, patch, MagicMock
from blocks_download import FileManager
from jsonschema import validate, ValidationError


@pytest.fixture
def mock_config():
    mock_config = MagicMock()
    mock_config.BLOCKS_DATA_DIR = "/mocked/path"
    return mock_config

@pytest.fixture
def file_manager(mock_config):
    return FileManager(mock_config)


# SAVE_TO_JSON #
# mock_open to simulate real file opening #
@pytest.mark.unit
@patch("builtins.open", new_callable=mock_open)
@patch("json.dump")
def test_save_to_json_success(mock_json_dump, mock_open_func, file_manager, caplog):
    data = {"key": "value"}
    filename = "test.json"
    file_path = "/mocked/path/to/file.json"

    with patch.object(FileManager, '_get_file_path', return_value=file_path) as mock_get_path:
        with patch("blocks_download.Utils.check_empty_result") as mock_check_empty:
            with caplog.at_level(logging.DEBUG):
                file_manager.save_to_json(data, filename)

            mock_get_path.assert_called_once_with(filename)
            mock_check_empty.assert_called_once_with(data, "data to save")
            mock_open_func.assert_called_once_with(file_path, "w")
            mock_json_dump.assert_called_once_with(data, mock_open_func(), indent=4)
            assert "Block data saved to JSON file" in caplog.text


@pytest.mark.unit
@patch("builtins.open", new_callable=mock_open, read_data='{"key": "value"}')
@patch("json.load", return_value={"key": "value"})
def test_load_from_json_success(mock_json_load, mock_open_func, file_manager, caplog):
    filename = "test.json"
    file_path = "/mocked/path/to/file.json"

    with patch.object(FileManager, '_get_file_path', return_value=file_path) as mock_get_path:
        with caplog.at_level(logging.DEBUG):
            result = file_manager.load_from_json(filename)

        mock_get_path.assert_called_once_with(filename)
        mock_open_func.assert_called_once_with(file_path, 'r')
        mock_json_load.assert_called_once()
        assert result == {"key": "value"}
        assert f"Data loaded from JSON file: {file_path}" in caplog.text


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


@pytest.mark.unit
@patch("builtins.open", new_callable=mock_open, read_data=json.dumps(block_transaction_data))
def test_load_core_data_from_json_structure(mock_open_func, file_manager):
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



@pytest.mark.unit
@patch("builtins.open", new_callable=mock_open, read_data=json.dumps(block_transaction_data))
def test_load_from_json_schema(mock_open_func, file_manager):
    filename = "large_file.json"
    result = file_manager.load_from_json(filename)
    
    try:
        validate(instance=result, schema=schema)
    except ValidationError as e:
        pytest.fail(f"JSON schema validation error: {e.message}")


# GET_FILE_PATH #
@pytest.mark.unit
def test_get_file_path(file_manager, monkeypatch):
    test_dir = '/test/directory'
    monkeypatch.setattr(file_manager.config, 'BLOCKS_DATA_DIR', test_dir)

    filename = "test.json"
    expected_path = os.path.join(test_dir, filename)
    assert file_manager._get_file_path(filename) == expected_path


# REMOVE_FILE #
@pytest.mark.unit
def test_remove_file_exists(file_manager, caplog):
    with patch("os.path.exists", return_value=True), patch("os.remove") as mock_remove:
        file_path = "dummy_path"
        file_manager.remove_file(file_path)

        mock_remove.assert_called_once_with(file_path)
        assert "Removed: dummy_path" in caplog.text


@pytest.mark.unit
def test_remove_file_not_exists(file_manager, caplog):
    with patch("os.path.exists", return_value=False), patch("os.remove") as mock_remove:
        file_path = "dummy_path"
        file_manager.remove_file(file_path)

        mock_remove.assert_not_called()
        assert "File not found, skipping removal: dummy_path" in caplog.text