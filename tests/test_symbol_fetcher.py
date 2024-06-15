import unittest
from unittest.mock import patch, mock_open, MagicMock
from stock_data_fetcher.symbol_fetcher import SymbolFetcher
import json
from pathlib import Path

class TestSymbolFetcher(unittest.TestCase):

    def setUp(self):
        self.sf = SymbolFetcher()

    @patch('stock_data_fetcher.symbol_fetcher.requests.get')
    def test_fetch_exchange_symbols(self, mock_get):
        mock_response = MagicMock()
        expected_json = {"data": {"rows": [{"symbol": "AAPL"}]}}
        mock_response.json.return_value = expected_json
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = self.sf.fetch_exchange_symbols('nasdaq')
        self.assertEqual(result, expected_json)

    @patch('stock_data_fetcher.symbol_fetcher.Path.open', new_callable=mock_open, read_data='[]')
    def test_load_exchange_symbols(self, mock_file):
        file_path = self.sf.symbols_dir / 'nasdaq' / 'nasdaq_full_symbols.json'
        with patch.object(Path, 'exists', return_value=True):
            result = self.sf.load_exchange_symbols('nasdaq')
            self.assertEqual(result, [])

    @patch('stock_data_fetcher.symbol_fetcher.Path.open', new_callable=mock_open)
    def test_save_exchange_symbols(self, mock_file):
        symbols_data = {"data": {"rows": [{"symbol": "AAPL"}]}}
        with patch.object(Path, 'mkdir', return_value=None), \
                patch('stock_data_fetcher.symbol_fetcher.json.dump') as mock_json_dump:
            self.sf.save_exchange_symbols(symbols_data, 'nasdaq')
            mock_json_dump.assert_called()

    @patch('stock_data_fetcher.symbol_fetcher.Path.open', new_callable=mock_open, read_data="AAPL\nGOOGL\n")
    def test_load_all_symbols(self, mock_file):
        with patch.object(Path, 'exists', return_value=True):
            result = self.sf.load_all_symbols()
            self.assertEqual(result, ["AAPL", "GOOGL"])

    @patch('builtins.open', new_callable=mock_open)
    @patch('stock_data_fetcher.symbol_fetcher.Path.exists', return_value=True)
    def test_concatenate_and_deduplicate_symbols(self, mock_exists, mock_open_func):
        # Mock file handles
        mock_file_handle = mock_open_func.return_value.__enter__.return_value
        mock_file_handle.write = MagicMock()

        # Call the method under test
        self.sf.concatenate_and_deduplicate_symbols(['nasdaq', 'nyse'], 'all_symbols.txt')

        # Verify that the correct file was opened for writing
        output_file = self.sf.symbols_dir / 'all' / 'all_symbols.txt'
        mock_open_func.assert_called_with(output_file, 'w')

        # Verify that write was called on the file handle
        mock_file_handle.write.assert_called()

if __name__ == "__main__":
    unittest.main()
