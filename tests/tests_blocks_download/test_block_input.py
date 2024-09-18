import pytest
from unittest.mock import patch, call
from blocks_download import BlockInput


class TestBlockInput:
    """
    Tests for the `BlockInput` class and its method `get_num_blocks_to_fetch`.
    """

    # get_num_blocks_to_fetch console tests #

    @patch('builtins.input', side_effect=['5'])
    @patch('blocks_download.logger', autospec=True)
    def test_get_num_blocks_to_fetch_console_correct_input(self, mock_logger, mock_input):
        """
        Test `get_num_blocks_to_fetch` method with valid console input.

        This test checks that:
        - The method correctly processes valid input provided via the console.
        - The method logs the appropriate information.
        - The input function is called exactly once.

        Parameters
        ----------
        mock_logger : unittest.mock.Mock
            The mock logger object used to verify logging behavior.
        mock_input : unittest.mock.Mock
            The mock input function used to simulate user input.

        Returns
        -------
        None
        """
        result = BlockInput.get_num_blocks_to_fetch(method='console')
        
        assert result == 5
        assert mock_input.call_count == 1
        
        expected_calls = [
            call('Attempting to get number of blocks to fetch using method: console'),
            call('User input for number of blocks: 5')
        ]
        
        mock_logger.info.assert_has_calls(expected_calls)


    @patch('builtins.input', side_effect=['-1'])
    @patch('blocks_download.logger', autospec=True)
    def test_get_num_blocks_to_fetch_logging_after_one_invalid_attempt(self, mock_logger, mock_input):
        """
        Test `get_num_blocks_to_fetch` method with invalid input and one attempt.

        This test verifies that:
        - The method logs the appropriate error message for invalid input.
        - The method logs the appropriate error message when the maximum number of attempts is reached.
        - The method raises a `ValueError` when the maximum number of attempts is reached.

        Parameters
        ----------
        mock_logger : unittest.mock.Mock
            The mock logger object used to verify logging behavior.
        mock_input : unittest.mock.Mock
            The mock input function used to simulate user input.

        Raises
        ------
        ValueError
            If the input is invalid and the maximum number of attempts is reached.

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Maximum number of attempts reached."):
            BlockInput.get_num_blocks_to_fetch(method='console', max_attempts=1)
        
        mock_logger.info.assert_called_once_with('Attempting to get number of blocks to fetch using method: console')    
        expected_error_calls = [
            call('Invalid input, please enter an integer! (Number of blocks must be greater than 0.)'),
            call('Maximum number of attempts reached.')
        ]

        mock_logger.error.assert_has_calls(expected_error_calls)


    @patch('builtins.input', side_effect=['-5', 'invalid', '0', '5'])  
    @patch('blocks_download.logger', autospec=True)
    def test_get_num_blocks_to_fetch_console_retries_until_valid(self, mock_logger, mock_input):
        """
        Test the `get_num_blocks_to_fetch` method with multiple invalid inputs before a valid input.

        This test verifies that:
        - The method correctly processes valid input after several invalid attempts.
        - The method logs appropriate error messages for each invalid input.
        - The method logs the correct information when a valid input is provided.
        - The input function is called the expected number of times.

        Parameters
        ----------
        mock_logger : unittest.mock.Mock
            The mock logger object used to verify logging behavior.
        mock_input : unittest.mock.Mock
            The mock input function used to simulate user input.

        Returns
        -------
        None
        """
        result = BlockInput.get_num_blocks_to_fetch(method='console')    
        
        assert result == 5   
        assert mock_input.call_count == 4
        
        expected_error_calls = [
            call('Invalid input, please enter an integer! (Number of blocks must be greater than 0.)'),
            call('Invalid input, please enter an integer! (invalid literal for int() with base 10: \'invalid\')'),
            call('Invalid input, please enter an integer! (Number of blocks must be greater than 0.)')
        ]    
        
        expected_info_calls = [
            call('Attempting to get number of blocks to fetch using method: console'),
            call('User input for number of blocks: 5')
        ]
            
        mock_logger.error.assert_has_calls(expected_error_calls)        
        mock_logger.info.assert_has_calls(expected_info_calls)
            
        assert mock_logger.info.call_count == len(expected_info_calls)
        assert mock_logger.error.call_count == len(expected_error_calls)


    @patch('builtins.input', side_effect=['-5', 'invalid', '0', 's', '-2'])  
    @patch('blocks_download.logger', autospec=True)
    def test_get_num_blocks_to_fetch_console_max_attempts_failed(self, mock_logger, mock_input):
        """
        Test the `get_num_blocks_to_fetch` method with multiple invalid inputs exceeding the maximum attempts.

        This test checks that:
        - The method logs appropriate error messages for each invalid input.
        - The method logs an error message when the maximum number of attempts is reached.
        - The method handles and logs various invalid input errors correctly, such as non-numeric input.
        - The method raises a `ValueError` when the maximum number of attempts is exceeded.        

        It specifically verifies:
        - Error messages are logged for invalid inputs like negative numbers, non-numeric strings, and zero.
        - `ValueError` is raised and logged appropriately when the maximum attempts are reached.

        Parameters
        ----------
        mock_logger : unittest.mock.Mock
            The mock logger object used to verify logging behavior.
        mock_input : unittest.mock.Mock
            The mock input function used to simulate user input.

        Raises
        ------
        ValueError
            If the maximum number of attempts is reached with invalid inputs.

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match='Maximum number of attempts reached.'):
            BlockInput.get_num_blocks_to_fetch(method='console', max_attempts=5)    
    
        mock_logger.info.assert_called_once_with('Attempting to get number of blocks to fetch using method: console')       
        expected_error_calls = [
            call('Invalid input, please enter an integer! (Number of blocks must be greater than 0.)'),
            call('Invalid input, please enter an integer! (invalid literal for int() with base 10: \'invalid\')'),
            call('Invalid input, please enter an integer! (Number of blocks must be greater than 0.)'),
            call('Invalid input, please enter an integer! (invalid literal for int() with base 10: \'s\')'),
            call('Invalid input, please enter an integer! (Number of blocks must be greater than 0.)'),
            call('Maximum number of attempts reached.')
        ]    
        
        mock_logger.error.assert_has_calls(expected_error_calls)   
        assert mock_logger.error.call_count == len(expected_error_calls)

    # get_num_block_to_fetch interface tests #  

    @patch('blocks_download.QInputDialog.getInt', return_value=(10, True))
    @patch('blocks_download.logger', autospec=True)
    def test_get_num_blocks_to_fetch_interface_correct_input(self, mock_logger, mock_getInt):
        """
        Test the `get_num_blocks_to_fetch` method with valid interface input.

        This test checks that:
        - The method correctly processes a valid number of blocks entered via the interface.
        - The method logs the appropriate informational messages when a valid input is received.
        
        Parameters
        ----------
        mock_logger : unittest.mock.Mock
            The mock logger object used to verify logging behavior.
        mock_getInt : unittest.mock.Mock
            The mock QInputDialog.getInt function used to simulate user input via the interface.

        Returns
        -------
        None
        """
        result = BlockInput.get_num_blocks_to_fetch(method='interface')
        assert result == (10, True)

        expected_calls = [
            call('Attempting to get number of blocks to fetch using method: interface'),
            call('User input for number of blocks via interface: 10')
        ]
        mock_logger.info.assert_has_calls(expected_calls)
        assert mock_logger.info.call_count == 2


    @patch('blocks_download.QInputDialog.getInt', return_value=(0, False))
    @patch('blocks_download.logger', autospec=True)
    def test_get_num_blocks_to_fetch_interface_user_cancelled(self, mock_logger, mock_getInt):
        """
        Test the `get_num_blocks_to_fetch` method when the user cancels the input via the interface.

        This test checks that:
        - The method returns `None` when the user cancels the input.
        - The method logs the appropriate information when the user cancels the input.

        Parameters
        ----------
        mock_logger : unittest.mock.Mock
            The mock logger object used to verify logging behavior.
        mock_getInt : unittest.mock.Mock
            The mock interface function used to simulate user input.

        Returns
        -------
        None
        """
        result = BlockInput.get_num_blocks_to_fetch(method='interface')
        
        assert result is None
        
        expected_info_calls = [
            call('Attempting to get number of blocks to fetch using method: interface'),
            call('User cancelled input via interface.')
        ]
        
        mock_logger.info.assert_has_calls(expected_info_calls)
        
        assert mock_logger.info.call_count == 2


    @patch('blocks_download.QInputDialog.getInt', return_value=(-5, True))
    @patch('blocks_download.logger', autospec=True)
    def test_get_num_blocks_to_fetch_interface_invalid_input(self, mock_logger, mock_getInt):    
        """
        Test the `get_num_blocks_to_fetch` method with invalid input via the interface.

        This test checks that:
        - The method raises a `ValueError` when the input via the interface is invalid (e.g., negative number).
        - The method logs an error message when invalid input is provided.

        Parameters
        ----------
        mock_logger : unittest.mock.Mock
            The mock logger object used to verify logging behavior.
        mock_getInt : unittest.mock.Mock
            The mock interface function used to simulate user input.

        Raises
        ------
        ValueError
            If the input is invalid (a number less than or equal to 0).

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Number of blocks must be greater than 0."):
            BlockInput.get_num_blocks_to_fetch(method='interface')

        mock_logger.info.assert_called_once_with('Attempting to get number of blocks to fetch using method: interface')        
        mock_logger.error.assert_called_once_with('Problem with interface: Number of blocks must be greater than 0.')


    @patch('blocks_download.QInputDialog.getInt', side_effect=Exception('Unexpected error'))
    @patch('blocks_download.logger', autospec=True)
    def test_get_num_blocks_to_fetch_interface_exception(self, mock_logger, mock_getInt):    
        """
        Test the `get_num_blocks_to_fetch` method when an exception occurs during interface input.

        This test checks that:
        - The method raises an appropriate exception when an unexpected error occurs.
        - The method logs the exception details as an error.

        Parameters
        ----------
        mock_logger : unittest.mock.Mock
            The mock logger object used to verify logging behavior.
        mock_getInt : unittest.mock.Mock
            The mock interface function used to simulate user input.

        Raises
        ------
        Exception
            If an unexpected error occurs during input.

        Returns
        -------
        None
        """
        with pytest.raises(Exception, match="Unexpected error"):
            BlockInput.get_num_blocks_to_fetch(method='interface')
    
        mock_logger.info.assert_called_once_with('Attempting to get number of blocks to fetch using method: interface')
        mock_logger.error.assert_called_once_with('Problem with interface: Unexpected error')

    # get_num_block_to_fetch correct method tests #  

    @patch('blocks_download.logger', autospec=True)
    def test_get_num_blocks_to_fetch_invalid_method(self, mock_logger):   
        """
        Test the `get_num_blocks_to_fetch` method with an invalid method argument.

        This test checks that:
        - The method raises a `ValueError` when an invalid method argument is provided.
        - The method logs an error message indicating that the method is invalid.

        Parameters
        ----------
        mock_logger : unittest.mock.Mock
            The mock logger object used to verify logging behavior.

        Raises
        ------
        ValueError
            If an invalid method argument is provided.

        Returns
        -------
        None
        """ 
        with pytest.raises(ValueError, match="Invalid method, use 'console' or 'interface'."):
            BlockInput.get_num_blocks_to_fetch(method='invalid_method')    
        mock_logger.error.assert_called_once_with("Invalid method, use 'console' or 'interface'.")