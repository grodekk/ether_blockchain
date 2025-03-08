import pytest
from unittest.mock import patch, call, MagicMock
from blocks_download import BlockInput
from error_handler import ErrorHandler, CustomProcessingError
import logging
from PyQt5.QtWidgets import QInputDialog

@pytest.fixture
def block_input_console():
    return BlockInput(method="console")

class TestBlockInput:

    # get_user_input #
    @patch("builtins.input", return_value="5")
    def test_get_user_input_success(self, mock_input, block_input_console):
        result = block_input_console.get_user_input()
        assert result == 5


    @patch("builtins.input", return_value="abc")
    def test_get_user_input_error(self, mock_input, block_input_console, caplog, capsys):
        with caplog.at_level(logging.ERROR):
            with pytest.raises(CustomProcessingError):
                block_input_console.get_user_input()

        captured = capsys.readouterr()
        assert "BlockInput.get_user_input" in caplog.text
        assert "ValueError" in caplog.text
        assert "The entered value is not an integer." in captured.out


    # validate_input #
    def test_validate_input_success(self, block_input_console):
        result = block_input_console.validate_input(5)
        assert result == 5


    def test_validate_input_error_zero(self, block_input_console, caplog, capsys):
        with caplog.at_level(logging.ERROR):
            with pytest.raises(CustomProcessingError, match="Number of blocks must be greater than 0."):
                block_input_console.validate_input(0)

        captured = capsys.readouterr()
        assert "BlockInput.validate_input" in caplog.text
        assert "ValueError" in caplog.text
        assert "Number of blocks must be greater than 0." in captured.out


    def test_validate_input_error_negative(self, block_input_console, caplog, capsys):
        with caplog.at_level(logging.ERROR):
            with pytest.raises(CustomProcessingError, match="Number of blocks must be greater than 0."):
                block_input_console.validate_input(-1)

        captured = capsys.readouterr()
        assert "BlockInput.validate_input" in caplog.text
        assert "ValueError" in caplog.text
        assert "Number of blocks must be greater than 0." in caplog.text
        assert "Number of blocks must be greater than 0." in captured.out


    def test_console_input_success(self, block_input_console):
        mock_get_user_input = MagicMock(return_value=5)
        mock_validate_input = MagicMock(return_value=5)

        block_input_console.get_user_input = mock_get_user_input
        block_input_console.validate_input = mock_validate_input

        result = block_input_console.console_input(max_attempts=3)

        assert result == 5

        mock_validate_input.assert_called_once_with(5)
        mock_get_user_input.assert_called_once()


    def test_console_input_invalid_input(self, block_input_console, caplog, capsys):
        mock_get_user_input = MagicMock(return_value=0)
        mock_validate_input = MagicMock(side_effect=CustomProcessingError(ValueError))

        block_input_console.get_user_input = mock_get_user_input
        block_input_console.validate_input = mock_validate_input

        with caplog.at_level(logging.ERROR):
            with pytest.raises(CustomProcessingError, match="Maximum number of attempts reached."):
                block_input_console.console_input(max_attempts=1)

        captured = capsys.readouterr()
        assert "BlockInput.console_input" in caplog.text
        assert "ValueError" in caplog.text
        assert "Maximum number of attempts reached." in caplog.text
        assert "Maximum number of attempts reached." in captured.out

        mock_validate_input.assert_called()
        mock_get_user_input.assert_called()


