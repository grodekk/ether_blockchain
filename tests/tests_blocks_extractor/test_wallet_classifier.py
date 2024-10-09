import pytest
from blocks_extractor import WalletClassifier
from unittest.mock import MagicMock
from error_handler import CustomProcessingError

@pytest.fixture
def wallet_classifier():
    return WalletClassifier()

class TestWalletClassifier:
    
    # tests classify_wallet #
    def test_classify_wallet_static(self):        
        assert WalletClassifier.classify_wallet(15000) == "Above 10000 ETH"
        assert WalletClassifier.classify_wallet(5000) == "1000-10000 ETH"
        assert WalletClassifier.classify_wallet(500) == "100-1000 ETH"
        assert WalletClassifier.classify_wallet(50) == "10-100 ETH"
        assert WalletClassifier.classify_wallet(5) == "1-10 ETH"
        assert WalletClassifier.classify_wallet(0.5) == "0.1-1 ETH"
        assert WalletClassifier.classify_wallet(0.05) == "Below 0.1 ETH"
    

    # tests classify_wallets #
    def test_classify_wallets_success(self, wallet_classifier, caplog):
        wallets_transactions = {
            "wallet1": [{"value": 2000}, {"value": 3000}],
            "wallet2": [{"value": 0.05}, {"value": 0.02}],
            "wallet3": [{"value": 150}],
        }
        
        expected_result = {
            "1000-10000 ETH": 1,
            "Below 0.1 ETH": 1,
            "100-1000 ETH": 1
        }

        with caplog.at_level("INFO"):
            result = wallet_classifier.classify_wallets(wallets_transactions)

        assert result == expected_result
        assert len(caplog.records) == 0


    def test_classify_wallets_key_error(self, wallet_classifier, caplog):        
        wallets_transactions = {
            "wallet1": [{"value": 1000}],
            "wallet2": [{"amount": 500}],
        }

        with caplog.at_level("ERROR"):
            with pytest.raises(CustomProcessingError) as exc_info:
                wallet_classifier.classify_wallets(wallets_transactions)        
        
        assert "WalletClassifier.classify_wallets - Missing key" in caplog.text
        assert "KeyError" in str(exc_info.value)


    def test_classify_wallets_type_error(self, wallet_classifier, caplog):        
        wallets_transactions = {
            "wallet1": [{"value": "not a number"}],
        }

        with caplog.at_level("ERROR"):
            with pytest.raises(CustomProcessingError) as exc_info:
                wallet_classifier.classify_wallets(wallets_transactions)        
        
        assert "WalletClassifier.classify_wallets - Type error" in caplog.text
        assert "TypeError" in str(exc_info.value)
    

    def test_classify_wallets_attribute_error(self, wallet_classifier, caplog):       
        wallets_transactions = ["not a dictionary"]
        
        with caplog.at_level("ERROR"):
            with pytest.raises(CustomProcessingError) as exc_info:
                wallet_classifier.classify_wallets(wallets_transactions)
                
        assert "WalletClassifier.classify_wallets - Attribute error" in caplog.text
        assert "AttributeError" in str(exc_info.value)


    def test_classify_wallets_unexpected_error(self, wallet_classifier, caplog):        
        wallets_transactions = {
            "wallet1": [{"value": 100}],
            "wallet2": [{"value": "not a number"}],
        }
        
        wallet_classifier.classify_wallet = MagicMock(side_effect=RuntimeError("Unexpected error during classification"))
 
        with caplog.at_level("ERROR"):
            with pytest.raises(CustomProcessingError) as exc_info:
                wallet_classifier.classify_wallets(wallets_transactions)
        
        assert "WalletClassifier.classify_wallets - Unexpected error : Unexpected error during classification" in caplog.text
        assert "General Exception" in str(exc_info.value)