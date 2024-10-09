import pytest
from unittest.mock import MagicMock, patch
from blocks_extractor import TopWalletsGenerator
from error_handler import CustomProcessingError

@pytest.fixture
def top_wallets_generator():
    return TopWalletsGenerator()


class TestTopWalletsGenerator:

    # tests get_top_wallets #
    def test_get_top_wallets_success(self, top_wallets_generator):        
        wallets_transactions = {
            "wallet1": [{"value": -200, "type": "sell"}],
            "wallet2": [{"value": -150, "type": "sell"}],
            "wallet3": [{"value": -100, "type": "sell"}],
            "wallet4": [{"value": -50, "type": "sell"}],
            "wallet5": [{"value": 200, "type": "buy"}],
            "wallet6": [{"value": 150, "type": "buy"}],
            "wallet7": [{"value": 100, "type": "buy"}],
            "wallet8": [{"value": 50, "type": "buy"}]
        }

        expected_output_buy = [
            {
                "wallet address": "wallet5",
                "biggest transaction type (buy/sell)": "buy",
                "biggest transaction amount in ether": 200,
                "wallet balance": 200
            },
            {
                "wallet address": "wallet6",
                "biggest transaction type (buy/sell)": "buy",
                "biggest transaction amount in ether": 150,
                "wallet balance": 150
            },
        ]

        expected_output_sell = [
            {
                "wallet address": "wallet1",
                "biggest transaction type (buy/sell)": "sell",
                "biggest transaction amount in ether": -200,
                "wallet balance": -200
            },
            {
                "wallet address": "wallet2",
                "biggest transaction type (buy/sell)": "sell",
                "biggest transaction amount in ether": -150,
                "wallet balance": -150
            }
        ]
        
        top_wallets_info_buy = top_wallets_generator.get_top_wallets(wallets_transactions, top_n=2, is_seller=False)        
        top_wallets_info_sell = top_wallets_generator.get_top_wallets(wallets_transactions, top_n=2, is_seller=True)

        assert top_wallets_info_buy == expected_output_buy
        assert top_wallets_info_sell == expected_output_sell


    def test_get_top_wallets_key_error(self, top_wallets_generator, caplog):        
        wallets_transactions = {
            "wallet1": [{"value": 100}],
            "wallet2": [{}],
        }
        
        with caplog.at_level("ERROR"):
            with pytest.raises(CustomProcessingError) as exc_info:
                top_wallets_generator.get_top_wallets(wallets_transactions)
        
        assert "TopWalletsGenerator.get_top_wallets - Missing key" in caplog.text
        assert "KeyError" in str(exc_info.value)


    def test_get_top_wallets_type_error(self, top_wallets_generator, caplog):        
        wallets_transactions = {
            "wallet1": [{"value": 100}],
            "wallet2": [{"value": None}],
        }
        
        with caplog.at_level("ERROR"):
            with pytest.raises(CustomProcessingError) as exc_info:
                top_wallets_generator.get_top_wallets(wallets_transactions)
        
        assert "TopWalletsGenerator.get_top_wallets - Type error" in caplog.text
        assert "TypeError" in str(exc_info.value)


    def test_get_top_wallets_attribute_error(self, top_wallets_generator, caplog):        
        wallets_transactions = ["not a dictionary"]
        
        with caplog.at_level("ERROR"):
            with pytest.raises(CustomProcessingError) as exc_info:
                top_wallets_generator.get_top_wallets(wallets_transactions)
        
        assert "TopWalletsGenerator.get_top_wallets - Attribute error" in caplog.text
        assert "AttributeError" in str(exc_info.value)


    def test_get_top_wallets_unexpected_error(self, top_wallets_generator, caplog):        
        wallets_transactions = {
            "wallet1": [{"value": -200, "type": "sell"}],
            "wallet2": [{"value": -150, "type": "sell"}],
        }       
    
        with patch('builtins.sum', side_effect=Exception("Unexpected error")):
            with caplog.at_level("ERROR"):
                with pytest.raises(CustomProcessingError) as exc_info:
                    top_wallets_generator.get_top_wallets(wallets_transactions)
        
        assert "TopWalletsGenerator.get_top_wallets - Unexpected error : Unexpected error" in caplog.text
        assert "General Exception" in str(exc_info.value)