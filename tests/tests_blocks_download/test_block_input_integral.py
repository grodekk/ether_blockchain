import pytest
from unittest.mock import patch, MagicMock
from blocks_download import BlockInput, CustomProcessingError
import logging


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
@pytest.mark.integration
def test_get_num_blocks_to_fetch_invalid_method_decorated(block_input_invalid, caplog):
    with pytest.raises(CustomProcessingError, match="Invalid method, use 'console' or 'interface'."):
        block_input_invalid.get_num_blocks_to_fetch()

    assert "ValueError" in caplog.text
    assert "Invalid method, use 'console' or 'interface'." in caplog.text


# CONSOLE_INPUT #
@pytest.mark.integration
def test_console_input_invalid_input_decorated(block_input_console, caplog, capsys):
    mock_get_user_input = MagicMock(return_value=0)
    mock_validate_input = MagicMock(side_effect=CustomProcessingError(ValueError("invalid input")))

    block_input_console.get_user_input = mock_get_user_input
    block_input_console.validate_input_console = mock_validate_input

    with caplog.at_level(logging.ERROR):
        with pytest.raises(CustomProcessingError, match="Maximum number of attempts reached."):
            block_input_console.console_input(max_attempts=1)

    captured = capsys.readouterr()
    assert "ValueError" in caplog.text
    assert "Maximum number of attempts reached." in caplog.text
    assert "Maximum number of attempts reached." in captured.out

    mock_validate_input.assert_called()
    mock_get_user_input.assert_called()

#todo#
# INTERFACE_INPUT #

# GET_USER_INPUT #
@pytest.mark.integration
@patch("builtins.input", return_value="abc")
def test_get_user_input_error_decorated(mock_input, block_input_console, caplog, capsys):
    with caplog.at_level(logging.ERROR):
        with pytest.raises(CustomProcessingError):
            block_input_console.get_user_input()

    captured = capsys.readouterr()
    assert "ValueError" in caplog.text
    assert "The entered value is not an integer." in captured.out


@pytest.mark.integration
def test_validate_input_error_zero(block_input_console, caplog, capsys):
    with caplog.at_level(logging.ERROR):
        with pytest.raises(CustomProcessingError, match="Number of blocks must be greater than 0."):
            block_input_console.validate_input_console(0)

    captured = capsys.readouterr()
    assert "ValueError" in caplog.text
    assert "Number of blocks must be greater than 0." in captured.out


@pytest.mark.integration
def test_validate_input_error_negative(block_input_console, caplog, capsys):
    with caplog.at_level(logging.ERROR):
        with pytest.raises(CustomProcessingError, match="Number of blocks must be greater than 0."):
            block_input_console.validate_input_console(-1)

    captured = capsys.readouterr()
    assert "ValueError" in caplog.text
    assert "Number of blocks must be greater than 0." in caplog.text
    assert "Number of blocks must be greater than 0." in captured.out


@pytest.mark.class_integration
@patch("builtins.input", side_effect=["a", " ", "0.5", "*", "-1"])
def test_get_num_blocks_to_fetch_console_max_attempts_failed(mock_input, caplog, capsys, block_input_console):
    with pytest.raises(CustomProcessingError, match="Maximum number of attempts reached."):
        block_input_console.get_num_blocks_to_fetch(max_attempts=5)

    expected_fragments = [
        ("DEBUG", "blocks_download", "BlockInput", "get_num_blocks_to_fetch", "Attempting to get number of blocks to fetch using method: console"),
        ("DEBUG", "blocks_download", "BlockInput", "get_user_input", "User input:  "),
        ("DEBUG", "blocks_download", "BlockInput", "get_user_input", "User input: -1"),
        ("ERROR", "blocks_download", "BlockInput", "validate_input_console", "Number of blocks must be greater than 0."),
        ("ERROR", "blocks_download", "BlockInput", "get_user_input", "ValueError"),
        ("ERROR", "blocks_download", "BlockInput", "console_input", "Maximum number of attempts reached.")
    ]

    for level, module, class_name, func_name, fragment in expected_fragments:
        assert any(
            record.levelname == level and
            getattr(record, "custom_module", None) == module and
            getattr(record, "custom_className", None) == class_name and
            getattr(record, "custom_funcName", None) == func_name and
            fragment in record.message
            for record in caplog.records
        ), (
            f"Expected log entry not found:\n"  
            f"Level: {level}\n"
            f"Module: {module}\n"
            f"Class: {class_name}\n"
            f"Function: {func_name}\n"
            f"Message fragment: {fragment}\n"
            f"Available logs:\n"
            f"{''.join(f'- {r.levelname}: {r.message}\n' for r in caplog.records)}"
        )


    captured = capsys.readouterr()

    expected_output = "".join(
        "ERROR: The entered value is not an integer.\n"
        for _ in range(4)
    ) + "ERROR: Number of blocks must be greater than 0.\n" + "ERROR: Maximum number of attempts reached.\n"


    assert captured.out == expected_output, (
        f"Console output error: \n{captured.out}"
    )


@pytest.mark.class_integration
@patch("builtins.input", side_effect=["a", " ", "0.5", "*", "5"])
def test_get_num_blocks_to_fetch_console_max_attempts_success(mock_input, caplog, capsys, block_input_console):
    block_input_console.get_num_blocks_to_fetch(max_attempts=5)

    expected_fragments = [
        ("DEBUG", "blocks_download", "BlockInput", "get_num_blocks_to_fetch",
         "Attempting to get number of blocks to fetch using method: console"),
        ("DEBUG", "blocks_download", "BlockInput", "get_user_input", "User input: *"),
        ("DEBUG", "blocks_download", "BlockInput", "get_user_input", "User input: 5"),
        ("ERROR", "blocks_download", "BlockInput", "get_user_input", "ValueError"),
    ]

    for level, module, class_name, func_name, fragment in expected_fragments:
        assert any(
            record.levelname == level and
            getattr(record, "custom_module", None) == module and
            getattr(record, "custom_className", None) == class_name and
            getattr(record, "custom_funcName", None) == func_name and
            fragment in record.message
            for record in caplog.records
        ), (
            f"Expected log entry not found:\n"
            f"Level: {level}\n"
            f"Module: {module}\n"
            f"Class: {class_name}\n"
            f"Function: {func_name}\n"
            f"Message fragment: {fragment}\n"
            f"Available logs:\n"
            f"{''.join(f'- {r.levelname}: {r.message}\n' for r in caplog.records)}"

        )

    captured = capsys.readouterr()

    expected_output = "".join(
        "ERROR: The entered value is not an integer.\n"
        for _ in range(4)
    )

    assert captured.out == expected_output, (
        f"Console output error: \n{captured.out}"
    )