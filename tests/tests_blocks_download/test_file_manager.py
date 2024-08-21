import json
import pytest
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
def test_save_to_json_success(mock_open, file_manager):
    data = {"key": "value"}
    filename = "test.json"    
    file_manager.save_to_json(data, filename)           
    written_data = "".join(call.args[0] for call in mock_open().write.call_args_list)    
    expected_data = json.dumps(data, indent=4)
    assert written_data == expected_data

@patch("builtins.open", new_callable=mock_open)
def test_save_to_json_empty_data(mock_open, file_manager):
    data = {}
    filename = "empty.json"
    file_manager.save_to_json(data, filename)
    written_data = "".join(call.args[0] for call in mock_open().write.call_args_list)
    expected_data = json.dumps(data, indent=4)
    assert written_data == expected_data

@patch("builtins.open", new_callable=mock_open)
def test_save_to_json_large_data(mock_open, file_manager):
    data = {"key": "value" * 10000}  
    filename = "large.json"
    file_manager.save_to_json(data, filename)
    written_data = "".join(call.args[0] for call in mock_open().write.call_args_list)
    expected_data = json.dumps(data, indent=4)
    assert written_data == expected_data

@patch("builtins.open", new_callable=mock_open)
def test_save_to_json_nested_data(mock_open, file_manager):
    data = {"outer_key": {"inner_key": "value"}}
    filename = "nested.json"
    file_manager.save_to_json(data, filename)
    written_data = "".join(call.args[0] for call in mock_open().write.call_args_list)
    expected_data = json.dumps(data, indent=4)
    assert written_data == expected_data

@patch("builtins.open", new_callable=mock_open)
@patch("json.dump", side_effect=OSError("Failed to write"))
def test_save_to_json_oserror(mock_json_dump, mock_open, file_manager):   
    data = {"key": "value"}
    filename = "test.json"       
    with pytest.raises(OSError):
        file_manager.save_to_json(data, filename)
      
@patch("builtins.open", new_callable=mock_open)
@patch("json.dump", side_effect=IOError("Failed to write"))
def test_save_to_json_ioerror(mock_json_dump, mock_open, file_manager):    
    data = {"key": "value"}
    filename = "test.json" 
    with pytest.raises(IOError):
        file_manager.save_to_json(data, filename)


# load_from_json tests #      
@patch("builtins.open", new_callable=mock_open, read_data=json.dumps({"key": "value"}))
def test_load_from_json_success(mock_open, file_manager):
    data = {"key": "value"}
    filename = "test.json"
    result = file_manager.load_from_json(filename)    
    assert result == data

@patch("builtins.open", new_callable=mock_open, read_data=json.dumps({}))
def test_load_from_json_empty_data(mock_open, file_manager):
    filename = "empty.json"
    result = file_manager.load_from_json(filename)
    assert result == {}

@patch("builtins.open", new_callable=mock_open, read_data=json.dumps({"key": "value" * 100000}))
def test_load_from_json_large_data(mock_open, file_manager):
    filename = "large.json"
    result = file_manager.load_from_json(filename)
    expected_data = {"key": "value" * 100000}
    assert result == expected_data

@patch("builtins.open", new_callable=mock_open, read_data=json.dumps({"outer_key": {"inner_key": "value"}}))
def test_load_from_json_nested_data(mock_open, file_manager):
    filename = "nested.json"
    result = file_manager.load_from_json(filename)
    expected_data = {"outer_key": {"inner_key": "value"}}
    assert result == expected_data

@patch("builtins.open", new_callable=mock_open)
def test_load_from_json_file_not_found(mock_open, file_manager):
    filename = "nonexistent.json"       
    mock_open.side_effect = FileNotFoundError    
    result = file_manager.load_from_json(filename) 
    assert result == []

@patch("builtins.open", new_callable=mock_open, read_data="not a json")
@patch("json.load", side_effect=json.JSONDecodeError("Expecting value", "document", 0))
def test_load_from_json_json_decode_error(mock_json_load, mock_open, file_manager):
    filename = "corrupted.json"       
    result = file_manager.load_from_json(filename)    
    assert result == []

@patch("builtins.open", new_callable=mock_open)
@patch("json.load", side_effect=OSError("Failed to read"))
def test_load_from_json_oserror(mock_json_load, mock_open, file_manager):
    filename = "test.json"    
    result = file_manager.load_from_json(filename)
    assert result == []

@patch("builtins.open", new_callable=mock_open)
@patch("json.load", side_effect=IOError("Failed to read"))
def test_load_from_json_ioerror(mock_json_load, mock_open, file_manager):
    filename = "test.json"    
    result = file_manager.load_from_json(filename)
    assert result == []

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