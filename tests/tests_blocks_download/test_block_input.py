import pytest
from unittest.mock import patch
from blocks_download import BlockInput


# get_num_block_to_fetch console tests #  
@patch('builtins.input', return_value='5')
def test_get_num_blocks_to_fetch_console_valid(mock_input):
    result = BlockInput.get_num_blocks_to_fetch(method='console')
    assert result == 5

@patch('builtins.input', return_value='-5')
def test_get_num_blocks_to_fetch_console_negative(mock_input):
    result = BlockInput.get_num_blocks_to_fetch(method='console')
    assert result is None

@patch('builtins.input', return_value='0')
def test_get_num_blocks_to_fetch_console_zero(mock_input):
    result = BlockInput.get_num_blocks_to_fetch(method='console')
    assert result is None

@patch('builtins.input', return_value='invalid')
def test_get_num_blocks_to_fetch_console_invalid_input(mock_input):
    result = BlockInput.get_num_blocks_to_fetch(method='console')
    assert result is None

# get_num_block_to_fetch interface tests #  
@patch('PyQt5.QtWidgets.QInputDialog.getInt', return_value=(10, True))
def test_get_num_blocks_to_fetch_interface_valid(mock_getInt):
    result = BlockInput.get_num_blocks_to_fetch(method='interface')
    assert result == 10

@patch('PyQt5.QtWidgets.QInputDialog.getInt', return_value=(None, False))
def test_get_num_blocks_to_fetch_interface_cancelled(mock_getInt):
    result = BlockInput.get_num_blocks_to_fetch(method='interface')
    assert result is None

@patch('PyQt5.QtWidgets.QInputDialog.getInt', return_value=(-10, True))
def test_get_num_blocks_to_fetch_interface_negative(mock_getInt):
    result = BlockInput.get_num_blocks_to_fetch(method='interface')
    assert result is None

@patch('PyQt5.QtWidgets.QInputDialog.getInt', return_value=(0, True))
def test_get_num_blocks_to_fetch_interface_zero(mock_getInt):
    result = BlockInput.get_num_blocks_to_fetch(method='interface')
    assert result is None

def test_get_num_blocks_to_fetch_invalid_method():
    with pytest.raises(ValueError):
        BlockInput.get_num_blocks_to_fetch(method='invalid')
