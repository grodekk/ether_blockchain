import pytest
from blocks_extractor import WalletUpdater
from unittest.mock import MagicMock
from error_handler import CustomProcessingError

@pytest.fixture
def updater():
    return WalletUpdater()


class TestWalletUpdater:

    # tests update_wallets #
    def test_update_wallets_success(self, updater, caplog):
        with caplog.at_level("INFO"):
            updater.update_wallets("0xSender", "0xReceiver", 1.0)
   
            assert updater.wallets_transactions["0xSender"] == [{"value": -1.0, "type": "sell"}]
            assert updater.wallets_transactions["0xReceiver"] == [{"value": 1.0, "type": "buy"}]


    def test_update_wallets_invalid_type(self, updater, caplog):
        with caplog.at_level("ERROR"):
            with pytest.raises(CustomProcessingError) as exc_info:
                updater.update_wallets("0xSender", "0xReceiver", "1.0")

            assert "WalletUpdater.update_wallets - Type error" in caplog.text
            assert "TypeError" in str(exc_info.value)


    def test_update_wallets_unexpected_error(self, updater, caplog):                     
        updater.wallets_transactions = MagicMock()        
        updater.wallets_transactions.__getitem__.side_effect = Exception("Unexpected error during wallets transactions")
        
        with caplog.at_level("ERROR"):
            with pytest.raises(CustomProcessingError) as exc_info:
                updater.update_wallets("0xSender", "0xReceiver", 1.0)
            
            assert "WalletUpdater.update_wallets - Unexpected error" in caplog.text
            assert "General Exception" in str(exc_info.value)