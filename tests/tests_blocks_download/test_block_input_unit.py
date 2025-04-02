import pytest
from unittest.mock import patch
from blocks_download import BlockInput, CustomProcessingError
import logging
from dotenv import load_dotenv
load_dotenv()


@pytest.fixture
def block_input_console():
    return BlockInput(method="console")

@pytest.fixture
def block_input_interface():
    return BlockInput(method="interface")

@pytest.fixture
def block_input_invalid():
    return BlockInput(method="invalid")


# GET_NUM_BLOCK_TO_FETCH #
@pytest.mark.unit
@patch.object(BlockInput, 'console_input', return_value=5)
def test_get_num_blocks_to_fetch_console(mock_console_input, block_input_console, caplog):
    with caplog.at_level(logging.DEBUG):
        result = block_input_console.get_num_blocks_to_fetch()

    assert result == 5
    assert "Attempting to get number of blocks to fetch using method: console" in caplog.text
    mock_console_input.assert_called_once()

@pytest.mark.unit
@patch.object(BlockInput, 'interface_input', return_value=(5, True))
@patch.object(BlockInput, 'console_input')
def test_get_num_blocks_to_fetch_interface(
    mock_console_input, mock_interface_input, block_input_interface, caplog):
    with caplog.at_level(logging.DEBUG):
        result = block_input_interface.get_num_blocks_to_fetch()

    assert result == (5, True)
    assert "Attempting to get number of blocks to fetch using method: interface" in caplog.text
    mock_interface_input.assert_called_once()
    mock_console_input.assert_not_called()

@pytest.mark.unit
@patch.object(BlockInput, 'console_input')
@patch.object(BlockInput, 'interface_input')
def test_get_num_blocks_to_fetch_error(mock_console_input, mock_interface_input, block_input_invalid, caplog):
    with pytest.raises(ValueError, match="Invalid method, use 'console' or 'interface'."):
        block_input_invalid.get_num_blocks_to_fetch()

    mock_console_input.assert_not_called()
    mock_interface_input.assert_not_called()


# # CONSOLE_INPUT #
@pytest.mark.unit
@patch.object(BlockInput, 'validate_input', return_value=5)
@patch.object(BlockInput, 'get_user_input', return_value=5)
def test_console_input_success(mock_get_user_input, mock_validate_input, block_input_console):
    result = block_input_console.console_input(max_attempts=3)

    assert result == 5

    mock_validate_input.assert_called_once_with(5)
    mock_get_user_input.assert_called_once()

@pytest.mark.unit
@patch.object(BlockInput, 'validate_input', side_effect=[CustomProcessingError(ValueError("error")), 5])
@patch.object(BlockInput, 'get_user_input', return_value=[-1, 5])
def test_console_input_retry_error(mock_get_user_input, mock_validate_input, block_input_console):
    result = block_input_console.console_input(max_attempts=3)

    mock_validate_input.assert_called_with([-1, 5])
    assert mock_get_user_input.call_count == 2
    assert mock_validate_input.call_count == 2
    assert result == 5

@pytest.mark.unit
@patch.object(BlockInput, 'validate_input', side_effect=CustomProcessingError(ValueError("error")))
@patch.object(BlockInput, 'get_user_input', side_effect=CustomProcessingError(ValueError("error")))
def test_console_input_max_retries_error(mock_get_user_input, mock_validate_input, block_input_console):
    with pytest.raises(ValueError, match="Maximum number of attempts reached."):
        block_input_console.console_input(max_attempts=3)

    assert mock_get_user_input.call_count == 3
    assert mock_validate_input.call_count == 0


# INTERFACE_INPUT #
@pytest.mark.unit
@patch.object(BlockInput, 'validate_input', return_value=5)
@patch('PyQt5.QtWidgets.QInputDialog.getInt', return_value=(5, True))
def test_interface_input_success(mock_getint, mock_validate_input, block_input_console, caplog):
    with caplog.at_level(logging.DEBUG):
        result = block_input_console.interface_input()

    assert result == 5
    assert "User input for number of blocks via interface: 5" in caplog.text
    mock_validate_input.assert_called_once_with(5)
    mock_getint.assert_called_once()

@pytest.mark.unit
@patch.object(BlockInput, 'validate_input')
@patch('PyQt5.QtWidgets.QInputDialog.getInt', return_value=(0, False))
def test_interface_input_cancelled(mock_getint, mock_validate_input, block_input_console, caplog):
    with caplog.at_level(logging.DEBUG):
        result = block_input_console.interface_input()

    assert result is None
    assert "User cancelled input via interface." in caplog.text
    mock_validate_input.assert_not_called()
    mock_getint.assert_called_once()

@pytest.mark.unit
@patch.object(BlockInput, 'validate_input', side_effect=CustomProcessingError(ValueError("error")))
@patch('PyQt5.QtWidgets.QInputDialog.getInt', return_value=(5, True))
def test_interface_input_validation_error(mock_get_input, mock_validate_input, block_input_console):
    with pytest.raises(CustomProcessingError):
        block_input_console.interface_input()

    mock_get_input.assert_called_once()
    mock_validate_input.assert_called_once_with(5)


# GET_USER_INPUT #
@pytest.mark.unit
@patch("builtins.input", return_value="5")
def test_get_user_input_success(input_mocker, block_input_console, caplog):
    with caplog.at_level("DEBUG"):
        result = block_input_console.get_user_input()

    assert result == 5
    assert "User input: 5" in caplog.text

@pytest.mark.unit
@patch("builtins.input", return_value="s")
def test_get_user_input_error(input_mocker, block_input_console):
    with pytest.raises(ValueError):
        BlockInput.get_user_input()


# # VALIDATE_INPUT #
@pytest.mark.unit
def test_validate_input_success(block_input_console):
    result = block_input_console.validate_input(5)
    assert result == 5

@pytest.mark.unit
def test_validate_input_zero_error(block_input_console):
    with pytest.raises(ValueError):
        block_input_console.validate_input(0)

@pytest.mark.unit
def test_validate_input_negative_error(block_input_console):
    with pytest.raises(ValueError):
        block_input_console.validate_input(-1)