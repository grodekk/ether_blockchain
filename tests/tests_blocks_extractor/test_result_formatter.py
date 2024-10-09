import pytest
from unittest.mock import MagicMock
from blocks_extractor import ResultFormatter  # Zamień 'your_module' na właściwy moduł
from config import Config
from error_handler import CustomProcessingError

@pytest.fixture
def top_wallets_generator():        
    mock_generator = MagicMock()        
    mock_generator.get_top_wallets.return_value = [
        {"wallet address": "wallet1", "biggest transaction type (buy/sell)": "buy", "biggest transaction amount in ether": 100, "wallet balance": 1000},
        {"wallet address": "wallet2", "biggest transaction type (buy/sell)": "sell", "biggest transaction amount in ether": 50, "wallet balance": 500}
    ]
    return mock_generator


class TestResultFormatter:

    # tests format_result #
    def test_format_result_success(self, top_wallets_generator):
        start_hour_str = "2024-10-03T12:00:00Z"
        total_transactions = 10
        total_fees = 5
        wallets_balances = {"wallet1": 1000, "wallet2": 500}
        wallets_transactions = {}
        
        result = ResultFormatter.format_result(start_hour_str, total_transactions, total_fees, wallets_balances, top_wallets_generator, wallets_transactions)
        
        expected_result = {
            "time": start_hour_str,
            "transactions number": total_transactions,
            "average transaction fee": total_fees / total_transactions if total_transactions > 0 else 0,
            "wallet classification in eth balance": wallets_balances,
            "top 5 buyers": top_wallets_generator.get_top_wallets(wallets_transactions, top_n=5, is_seller=False),
            "top 5 sellers": top_wallets_generator.get_top_wallets(wallets_transactions, top_n=5, is_seller=True)
        }

        assert result == expected_result


    def test_format_result_exception(self, top_wallets_generator, caplog):        
        top_wallets_generator.get_top_wallets.side_effect = Exception("Test exception")

        start_hour_str = "2024-10-03T12:00:00Z"
        total_transactions = 10
        total_fees = 5
        wallets_balances = {"wallet1": 1000, "wallet2": 500}
        wallets_transactions = {}
        
        with caplog.at_level('ERROR'):
            with pytest.raises(CustomProcessingError) as exc_info:
                ResultFormatter.format_result(start_hour_str, total_transactions, total_fees, wallets_balances, top_wallets_generator, wallets_transactions)

        assert "ResultFormatter.format_result - Unexpected error : Test exception" in caplog.text
        assert "General Exception" in str(exc_info.value)
