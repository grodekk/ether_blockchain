import pytest
from unittest.mock import MagicMock, patch, ANY
import logging
from blocks_download import MainBlockProcessor

class TestMainBlockProcessor:    
    # tests test_get_target_block_numbers_with_int_success#
    def test_get_target_block_numbers_with_int_success(self, caplog):    
        mock_api = MagicMock()    
        mock_api.get_latest_block_number.return_value = 100   
        mock_config = MagicMock()    
        main_block_processor = MainBlockProcessor(config=mock_config)
        main_block_processor.api = mock_api
        
        with caplog.at_level(logging.DEBUG):
            result = main_block_processor.get_target_block_numbers(5)    
        
        assert result == [99, 98, 97, 96, 95]        
        assert "MainBlockProcessor: Fetched latest block number" in caplog.text
        assert "Generated target block numbers from 99 to 95. Total count: 5" in caplog.text


    def test_get_target_block_numbers_with_list_success(self, caplog):    
        mock_api = MagicMock()    
        mock_config = MagicMock()
        block_processor = MainBlockProcessor(config=mock_config)
        block_processor.api = mock_api
        block_numbers = [100, 99, 98]

        with caplog.at_level(logging.DEBUG):
            result = block_processor.get_target_block_numbers(block_numbers)

        assert result == block_numbers
        
        assert "MainBlockProcessor: Fetched latest block number" in caplog.text
        assert "Received list of block numbers" in caplog.text


    def test_get_target_block_numbers_invalid_input_type(self, caplog):
        mock_api = MagicMock()    
        mock_api.get_latest_block_number.return_value = 100   
        mock_config = MagicMock()    
        block_processor = MainBlockProcessor(config=mock_config)
        block_processor.api = mock_api
        
        invalid_input = "invalid_input"

        with caplog.at_level(logging.ERROR):
            with pytest.raises(ValueError, match="Invalid input: must be either a list of block numbers or an integer number of blocks"):
                block_processor.get_target_block_numbers(invalid_input)
        
        assert any(
            "MainBlockProcessor: Failed to get target block numbers: Invalid input: must be either a list of block numbers or an integer number of blocks"
            in record.message and record.levelname == "ERROR"
            for record in caplog.records
        )


    def test_get_target_block_numbers_runtime_error(self, caplog):    
        mock_api = MagicMock()
        mock_api.get_latest_block_number.side_effect = Exception("Unexpected error")
        mock_config = MagicMock()
        block_processor = MainBlockProcessor(config=mock_config)
        block_processor.api = mock_api
        
        with pytest.raises(RuntimeError, match="MainBlockProcessor: Failed to get target block numbers: Unexpected error"):
            with caplog.at_level(logging.ERROR):
                block_processor.get_target_block_numbers(5)
        
        assert any(
            "MainBlockProcessor: Failed to get target block numbers: Unexpected error"
            in record.message and record.levelname == "ERROR"
            for record in caplog.records
        )


    # tests process_blocks#
    def test_process_blocks_success(self, caplog):    
        mock_file_manager = MagicMock()
        mock_block_processor = MagicMock()
        mock_processor = MagicMock()    
        mock_config = MagicMock()
        mock_config.BLOCKS_DATA_FILE = "blocks_data.json"    
        mock_file_manager.load_from_json.return_value = [1, 2, 3]
        
        block_processor = MainBlockProcessor(config=mock_config)
        block_processor.file_manager = mock_file_manager
        block_processor.block_processor.process_block = mock_block_processor.process_block
        mock_multi_processor = MagicMock()    
        
        with patch('blocks_download.MultiProcessor', return_value=mock_multi_processor):
            with caplog.at_level(logging.DEBUG):
                block_processor.process_blocks([4, 5, 6])
        
        mock_multi_processor.start.assert_called_once_with(
            target_block_numbers=[4, 5, 6],
            process_func=mock_block_processor.process_block,
            progress_callback=None,
            check_interrupt=None,
            fetched_block_numbers=[1, 2, 3],
            save_callback=ANY
        )
        
        assert "MainBlockProcessor: Starting block processing" in caplog.text
        assert "MainBlockProcessor: process_blocks executed successfully" in caplog.text

    def test_process_blocks_runtime_error(self, caplog):    
        mock_file_manager = MagicMock()
        mock_block_processor = MagicMock()
        mock_processor = MagicMock()    
        mock_config = MagicMock()
        mock_config.BLOCKS_DATA_FILE = "blocks_data.json"
    
        mock_block_processor.process_block.side_effect = Exception("Processing error")
        
        block_processor = MainBlockProcessor(config=mock_config)
        block_processor.file_manager = mock_file_manager
        block_processor.block_processor.process_block = mock_block_processor.process_block
        mock_multi_processor = MagicMock()
        
        with patch('blocks_download.MultiProcessor', return_value=mock_multi_processor):
            mock_multi_processor.start.side_effect = Exception("Processing error")
            
            with pytest.raises(RuntimeError, match="MainBlockProcessor: Error during block processing: Processing error"):
                with caplog.at_level(logging.ERROR):
                    block_processor.process_blocks([4, 5, 6])

        assert any(
            "MainBlockProcessor: Error during block processing: Processing error"
            in record.message and record.levelname == "ERROR"
            for record in caplog.records
        )


    # tests handle_missing_blocks #
    def test_handle_missing_blocks_no_missing_blocks(self, caplog):    
        mock_block_downloader = MagicMock()
        mock_config = MagicMock()
        mock_config.REQUEST_DELAY = 0.1

        block_processor = MainBlockProcessor(config=mock_config)
        block_processor.block_downloader = mock_block_downloader
        
        target_block_numbers = [1, 2, 3]
        fetched_block_numbers = [1, 2, 3]

        block_processor.handle_missing_blocks(target_block_numbers, fetched_block_numbers)
    
        mock_block_downloader.download_single_block.assert_not_called()
        assert "Missing blocks detected" not in caplog.text


    def test_handle_missing_blocks_with_missing_blocks(self, caplog):    
        mock_block_downloader = MagicMock()
        mock_config = MagicMock()
        mock_config.REQUEST_DELAY = 0.1

        block_processor = MainBlockProcessor(config=mock_config)
        block_processor.block_downloader = mock_block_downloader
        
        target_block_numbers = [1, 2, 3, 4]
        fetched_block_numbers = [1, 3]

        block_processor.handle_missing_blocks(target_block_numbers, fetched_block_numbers)
        
        mock_block_downloader.download_single_block.assert_any_call(2, fetched_block_numbers)
        mock_block_downloader.download_single_block.assert_any_call(4, fetched_block_numbers)
        assert mock_block_downloader.download_single_block.call_count == 2
        
        assert "Missing blocks detected: [2, 4]" in caplog.text


    def test_handle_missing_blocks_with_error_during_download(self, caplog):    
        mock_block_downloader = MagicMock()
        mock_block_downloader.download_single_block.side_effect = Exception("Download error")
        mock_config = MagicMock()
        mock_config.REQUEST_DELAY = 0.1

        block_processor = MainBlockProcessor(config=mock_config)
        block_processor.block_downloader = mock_block_downloader
        
        target_block_numbers = [1, 2, 3]
        fetched_block_numbers = [1]
        
        with pytest.raises(RuntimeError, match="Failed to download block 2"):
                block_processor.handle_missing_blocks(target_block_numbers, fetched_block_numbers)

        assert any(
            "Failed to download block 2: Download error"
            in record.message and record.levelname == "ERROR"
            for record in caplog.records
        )

        assert mock_block_downloader.download_single_block.call_count == 1

    # tests run #
    def test_run_success(self, caplog):    
        main_block_processor = MainBlockProcessor(config=MagicMock())
        main_block_processor.get_target_block_numbers = MagicMock(return_value=[1, 2, 3])
        main_block_processor.process_blocks = MagicMock()
        
        block_numbers_or_num_blocks = [1, 2, 3]
        
        with caplog.at_level(logging.INFO):
            main_block_processor.run(block_numbers_or_num_blocks)
        
        main_block_processor.get_target_block_numbers.assert_called_once_with(block_numbers_or_num_blocks)
        main_block_processor.process_blocks.assert_called_once_with([1, 2, 3], None, None)
        
        assert "Starting MainBlockProcessor run..." in caplog.text
        assert "MainBlockProcessor run completed." in caplog.text


    def test_run_failure(self, caplog):    
        main_block_processor = MainBlockProcessor(config=MagicMock())
        main_block_processor.get_target_block_numbers = MagicMock(side_effect=ValueError("Invalid input"))
        main_block_processor.process_blocks = MagicMock()

        block_numbers_or_num_blocks = [1, 2, 3]

        with caplog.at_level(logging.INFO):
            with pytest.raises(ValueError, match="Invalid input"):
                main_block_processor.run(block_numbers_or_num_blocks)

        main_block_processor.get_target_block_numbers.assert_called_once_with(block_numbers_or_num_blocks)
        
        assert "Starting MainBlockProcessor run..." in caplog.text
        assert "MainBlockProcessor run completed." not in caplog.text