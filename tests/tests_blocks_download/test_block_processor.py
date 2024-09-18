import pytest
from unittest.mock import MagicMock, patch
from blocks_download import BlockProcessor

@pytest.fixture
def setup_block_processor():    
    mock_api = MagicMock()
    mock_file_manager = MagicMock()
    mock_config = MagicMock()
    mock_config.REQUEST_DELAY = 2
    
    block_processor = BlockProcessor(mock_api, mock_file_manager, mock_config)
    return block_processor, mock_api, mock_file_manager

class TestBlocksProcessor:

    # process_block tests#
    def test_process_block_success(self, setup_block_processor, caplog):
        block_processor, mock_api, mock_file_manager = setup_block_processor
        
        mock_api.get_block_timestamp.return_value = 1723401311
        mock_api.get_block_transactions.return_value = [
            {"hash": "0xecf6efe5f8cd86fc7ea9bf9dd4e1617e65172c36d0e1ff8a30149f5339333dec"},
            {"hash": "0xc281d2fa303eb7a975f2e790e02be5d22d47459685486d55c8ecbd6063740e8a"},
            {"hash": "0x5c63a10656b11a94fb44762f53e834747529823dcd2a06425397ff95de0622c8"}        
        ]
        
        fetched_block_numbers = []
        
        with patch('time.sleep', return_value=None):
            block_number, result = block_processor.process_block(20507193, fetched_block_numbers, None)
            
            assert block_number == 20507193
            assert result == 1            

            mock_file_manager.save_to_json.assert_called_once_with({
                "block_number": 20507193,
                "timestamp": 1723401311,
                "transactions": [        
                    {"hash": "0xecf6efe5f8cd86fc7ea9bf9dd4e1617e65172c36d0e1ff8a30149f5339333dec"},
                    {"hash": "0xc281d2fa303eb7a975f2e790e02be5d22d47459685486d55c8ecbd6063740e8a"},
                    {"hash": "0x5c63a10656b11a94fb44762f53e834747529823dcd2a06425397ff95de0622c8"}
                ]
            }, "block_20507193.json")
            
            assert "Processing block: 20507193" in caplog.text
            assert "Block 20507193 saved to block_20507193.json" in caplog.text


    def test_process_block_invalid_block_number(self, setup_block_processor, caplog):
        block_processor, mock_api, mock_file_manager = setup_block_processor

        with pytest.raises(ValueError, match="Invalid block number: -1"):
            block_processor.process_block(-1, [], None)

        assert "Invalid block number: -1" in caplog.text


    def test_process_block_invalid_block_number_non_int(self, setup_block_processor, caplog):
        block_processor, mock_api, mock_file_manager = setup_block_processor

        with pytest.raises(ValueError, match="Invalid block number: invalid"):
            block_processor.process_block("invalid", [], None)
            
        assert "Invalid block number: invalid" in caplog.text


    def test_process_block_interrupt(self, setup_block_processor, caplog):
        block_processor, mock_api, mock_file_manager = setup_block_processor
        mock_interrupt_flag = MagicMock()
        mock_interrupt_flag.value = True
        
        block_number, result = block_processor.process_block(20507193, [], mock_interrupt_flag)
        
        assert block_number is None
        assert result == 0
        assert "Processing interrupted for block: 20507193" in caplog.text


    def test_process_block_already_fetched(self, setup_block_processor, caplog):
        block_processor, mock_api, mock_file_manager = setup_block_processor
        fetched_block_numbers = [20507193]
        
        block_number, result = block_processor.process_block(20507193, fetched_block_numbers, None)
        
        assert block_number is None
        assert result == 1
        assert "Processing block: 20507193" in caplog.text
        assert "Block 20507193 already fetched. Skipping..." in caplog.text


    def test_process_block_invalid_timestamp(self, setup_block_processor, caplog):
        block_processor, mock_api, mock_file_manager = setup_block_processor
        mock_api.get_block_timestamp.return_value = -1

        with pytest.raises(ValueError, match="Validation error for block 20507193"):
            block_processor.process_block(20507193, [], None)

        
        assert "Processing block: 20507193" in caplog.text
        assert "Invalid timestamp for block: 20507193" in caplog.text
        assert "Validation error in process_block for block 20507193: Invalid timestamp for block: 20507193" in caplog.text


    def test_process_block_invalid_transactions(self, setup_block_processor, caplog):
        block_processor, mock_api, mock_file_manager = setup_block_processor
        mock_api.get_block_timestamp.return_value = 1723401311
        mock_api.get_block_transactions.return_value = [
            {"wrong_key": "0xecf6efe5f8cd86fc7ea9bf9dd4e1617e65172c36d0e1ff8a30149f5339333dec"}
        ]
        
        with pytest.raises(ValueError, match="Validation error for block 20507193"):
            block_processor.process_block(20507193, [], None)

        assert "Processing block: 20507193" in caplog.text
        assert "Invalid transactions for block: 20507193" in caplog.text
        assert "Validation error in process_block for block 20507193: Invalid transactions for block: 20507193" in caplog.text


    def test_process_block_exception(self, setup_block_processor, caplog):
        block_processor, mock_api, mock_file_manager = setup_block_processor
        mock_api.get_block_timestamp.side_effect = Exception("Test exception")
        
        with pytest.raises(RuntimeError, match="Error while processing block 20507193"):
            block_processor.process_block(20507193, [], None)

        assert "Processing block: 20507193" in caplog.text
        assert "Unexpected error occurred in process_block for block 20507193: Test exception" in caplog.text