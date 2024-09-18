import pytest
from unittest.mock import MagicMock, patch, call
import logging
from multiprocessing import Pool, Manager, Lock
from blocks_download import MultiProcessor  # Zaktualizuj nazwę modułu zgodnie z nazwą pliku
import logging

# Ustawienia loggera
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.addHandler(handler)

class TestMultiProcessor:
    # apply_async tests # 
    def test_apply_async_success(self, caplog):
        processor = MultiProcessor()
        
        processor.pool.apply_async = MagicMock()
        
        def simple_func(x):
            return x

        args = (1, 2) 
        callback = MagicMock()

        with caplog.at_level(logging.INFO):
            processor.apply_async(simple_func, args=args, callback=callback)
        
        assert processor.pool.apply_async.called
        
        call_args = processor.pool.apply_async.call_args    

        args_tuple = call_args[0] 
        kwargs = call_args[1] 
        
        func = args_tuple[0]
        func_args = kwargs.get('args', None)  
        func_callback = kwargs.get('callback', None)  
        error_callback = kwargs.get('error_callback', None)  

        assert func == simple_func
        assert func_args == args 
        assert func_callback == callback  
        assert error_callback is None  


    def test_apply_async_logging_error(self, caplog):
        processor = MultiProcessor()
        processor.pool.apply_async = MagicMock(side_effect=Exception("Test Exception"))
        
        with caplog.at_level(logging.ERROR):
            with pytest.raises(RuntimeError, match="Failed to apply async function"):
                processor.apply_async(lambda x: x, args=())
        
        assert "Failed to apply async function" in caplog.text
        assert "Test Exception" in caplog.text

    def test_apply_async_error_callback_logging(self, caplog):
        processor = MultiProcessor()        
        processor.pool.apply_async = MagicMock(side_effect=Exception("Test Exception"))
        
        def error_callback(e):
            logger.error(f"Error occurred in async process: {e}")

        with caplog.at_level(logging.ERROR):            
            processor.apply_async(lambda x: x, args=(), error_callback=error_callback)
        
        assert "Error occurred in async process: Test Exception" in caplog.text
        

    # update_progress tests #
    def test_update_progress_basic_without_save_callback(self, caplog):
        processor = MultiProcessor()    
        
        progress_callback = MagicMock()
        save_callback = MagicMock()
        
        fetched_block_numbers = []
        total_target = 100
        x = (1, 10)  # block_number, progress_increment

        with caplog.at_level(logging.INFO):
            processor.update_progress(x, progress_callback, total_target, fetched_block_numbers, save_callback)
        
        progress_callback.assert_called_once_with(total_target, 10)
        save_callback.assert_not_called() 

        assert fetched_block_numbers == [1]    
        assert processor.total_processed_blocks.value == 10
        
        assert "Block 1 added to fetched_block_numbers." in caplog.text
        assert "Saving progress with 1 fetched blocks." not in caplog.text


    def test_update_progress_success_advanced_with_save_callback(self, caplog):
        processor = MultiProcessor()    
        
        progress_callback = MagicMock()
        save_callback = MagicMock()

        fetched_block_numbers = []
        total_target = 100

        for i in range(50):
            x = (i, 1)  # block_number, progress_increment
            with caplog.at_level(logging.INFO):
                processor.update_progress(x, progress_callback, total_target, fetched_block_numbers, save_callback)
        
        progress_callback.assert_called_with(total_target, 50)

        assert progress_callback.call_count == 50
        assert save_callback.call_count == 1
        assert len(fetched_block_numbers) == 50
        assert all(num in fetched_block_numbers for num in range(50))
        assert processor.total_processed_blocks.value == 50   
        assert all(f"Block {i} added to fetched_block_numbers." in caplog.text for i in range(50))
        assert "Saving progress with 50 fetched blocks." in caplog.text


    def test_update_progress_error_handling(self, caplog):
        processor = MultiProcessor()      
        progress_callback = MagicMock()
        save_callback = MagicMock()

        fetched_block_numbers = MagicMock()
        fetched_block_numbers.append.side_effect = Exception("Test Exception")       
        
        with caplog.at_level(logging.ERROR):
            with pytest.raises(RuntimeError, match="Failed to update progress"):
                processor.update_progress((0, 1), progress_callback, 100, fetched_block_numbers, save_callback)
        
        assert "Failed to update progress: Test Exception" in caplog.text   
        assert "Test Exception" in caplog.text


    # start tests #
    def test_start_success_with_logging(self, caplog):    
        processor = MultiProcessor()    
        processor.apply_async = MagicMock()
        processor.update_progress = MagicMock()
        processor.pool = MagicMock()
        processor.pool.close = MagicMock()
        processor.pool.join = MagicMock()    
        processor.interrupt_flag = MagicMock()

        progress_callback = MagicMock()
        save_callback = MagicMock()
        check_interrupt = MagicMock(return_value=False)  
        fetched_block_numbers = []
        
        target_block_numbers = [1, 2, 3]
        process_func = MagicMock()
        
        with caplog.at_level(logging.INFO):
            processor.start(target_block_numbers, process_func, progress_callback, check_interrupt, fetched_block_numbers, save_callback)
        
        for i, block_number in enumerate(target_block_numbers):
            args, kwargs = processor.apply_async.call_args_list[i]
            
            assert args[0] == process_func
            assert kwargs['args'] == (block_number, fetched_block_numbers, processor.interrupt_flag)
            
            assert callable(kwargs['callback'])
            assert callable(kwargs['error_callback'])
        
        for block_number in target_block_numbers:
            assert f"Adding block to process: {block_number}" in caplog.text
        
        assert processor.pool.close.called
        assert processor.pool.join.called


    def test_start_check_interrupt_error_handling(self, caplog):    
        processor = MultiProcessor()    
        processor.apply_async = MagicMock()
        processor.pool = MagicMock()    
        processor.interrupt_flag = MagicMock()
        progress_callback = MagicMock()
        save_callback = MagicMock()
        fetched_block_numbers = []
        
        check_interrupt = MagicMock(side_effect=Exception("Interrupt check failed"))

        target_block_numbers = [1, 2, 3]
        process_func = MagicMock()
        
        with caplog.at_level(logging.ERROR):
            with pytest.raises(RuntimeError, match="Failed to start processing block"):
                processor.start(target_block_numbers, process_func, progress_callback, check_interrupt, fetched_block_numbers, save_callback)
        
        assert "Check interrupt failed with error: Interrupt check failed" in caplog.text
        assert "Failed to start processing block 1: Interrupt check failed" in caplog.text


    def test_start_apply_async_error_handling(self, caplog):    
        processor = MultiProcessor()    
        processor.apply_async = MagicMock(side_effect=Exception("Async error occurred"))
        processor.pool = MagicMock()    
        processor.interrupt_flag = MagicMock()
        progress_callback = MagicMock()
        save_callback = MagicMock()
        check_interrupt = MagicMock(return_value=False)
        fetched_block_numbers = []

        target_block_numbers = [1, 2, 3]
        process_func = MagicMock()
        
        with caplog.at_level(logging.ERROR):
            with pytest.raises(RuntimeError, match="Failed to start processing block"):
                processor.start(target_block_numbers, process_func, progress_callback, check_interrupt, fetched_block_numbers, save_callback)
        
        assert "Failed to start processing block 1: Async error occurred" in caplog.text