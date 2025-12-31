import pytest
from unittest.mock import MagicMock
from blocks_extractor import TransactionProcessor
from error_handler import CustomProcessingError

@pytest.fixture
def processor():
    return TransactionProcessor()

class TestTransactionProcessor:
    
    # test process_transaction #
    def test_process_transaction_success(self, processor, caplog):        
        transaction = {
            "from": "0xSenderAddress",
            "to": "0xReceiverAddress",
            "value": "0xde0b6b3a7640000",  
            "gasPrice": "0x3b9aca00",      
            "gas": "0x5208"                
        }
        
        with caplog.at_level('INFO'):
            sender, receiver, value_eth = processor.process_transaction(transaction)
            
            assert sender == "0xSenderAddress"
            assert receiver == "0xReceiverAddress"
            assert value_eth == 1.0
            assert processor.total_transactions == 1
            assert processor.total_fees == 0.000021
            assert processor.total_value_eth == 1.0
        

    def test_process_transaction_missing_key(self, processor, caplog):        
        transaction = {
            "from": "0xSenderAddress",
            "to": "0xReceiverAddress",
            "gasPrice": "0x3b9aca00",
            "gas": "0x5208"
        }

        with caplog.at_level('ERROR'):
            with pytest.raises(CustomProcessingError) as exc_info:
                processor.process_transaction(transaction)

            assert "KeyError" in caplog.text
            assert "value" in caplog.text
    

    def test_process_transaction_invalid_value(self, processor, caplog):        
        transaction = {
            "from": "0xSenderAddress",
            "to": "0xReceiverAddress",
            "value": "invalid_value",
            "gasPrice": "0x3b9aca00",
            "gas": "0x5208"
        }

        with caplog.at_level("ERROR"):
            with pytest.raises(CustomProcessingError) as exc_info:
                processor.process_transaction(transaction)            
            
            assert "invalid_value" in caplog.text


    def test_process_transaction_type_error(self, processor, caplog):        
        transaction = {
            "from": "0xSenderAddress",
            "to": "0xReceiverAddress",
            "value": None,
            "gasPrice": "0x5",
            "gas": "0x10"
        }        

        with caplog.at_level('ERROR'):
            with pytest.raises(CustomProcessingError):
                processor.process_transaction(transaction)
        
        assert "TypeError" in caplog.text


    def test_process_transaction_unexpected_error(self, processor, caplog):
        transaction = MagicMock()
        transaction.__getitem__.side_effect = Exception("Unexpected error during transaction processing")
        with caplog.at_level("ERROR"):
            with pytest.raises(CustomProcessingError):
                processor.process_transaction(transaction)            
            
            assert "Unexpected error during transaction processing" in caplog.text