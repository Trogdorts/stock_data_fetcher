import requests
import json
import logging
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Callable
from stock_data_fetcher.logging_handler import LoggingHandler

LoggingHandler.setup_logging({"file": "symbol_fetcher.log"})

class SymbolFetcher:
    def __init__(self):
        """
        Initialize the SymbolFetcher instance.

        Sets up base directories, user agent, and exchanges.
        Ensures that necessary directories exist.
        """
        self.base_dir = Path(__file__).resolve().parent.parent
        self.data_dir = self.base_dir / 'data'
        self.symbols_dir = self.data_dir / 'symbols'
        self.fortune500_dir = self.symbols_dir / 'fortune500'
        self.user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:85.0) Gecko/20100101 Firefox/85.0'
        self.exchanges = ['nasdaq', 'nyse', 'amex']
        self._ensure_directories_exist()

    def _ensure_directories_exist(self):
        """
        Ensure that the necessary directories exist.

        Creates the symbols and fortune500 directories if they do not exist.
        """
        self.symbols_dir.mkdir(parents=True, exist_ok=True)
        self.fortune500_dir.mkdir(parents=True, exist_ok=True)

    def fetch_exchange_symbols(self, exchange: str) -> Optional[Dict]:
        """
        Fetch stock symbols from the API for a given exchange.

        Args:
            exchange (str): The exchange to fetch symbols for (e.g., 'nasdaq').

        Returns:
            Optional[Dict]: The JSON response from the API containing the symbols data,
                            or None if the request failed.
        """
        url = f"https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&offset=0&exchange={exchange}&download=true"
        headers = {'User-Agent': self.user_agent}
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Failed to fetch symbols for {exchange}: {e}")
            return None

    def save_exchange_symbols(self, symbols_data: Dict, exchange: str):
        """
        Save downloaded stock symbols to JSON and TXT files for a specific exchange.

        Args:
            symbols_data (Dict): The symbols data to save.
            exchange (str): The exchange for which the symbols are being saved.
        """
        exchange_dir = self.symbols_dir / exchange
        exchange_dir.mkdir(parents=True, exist_ok=True)
        full_symbols_path = exchange_dir / f"{exchange}_full_symbols.json"
        try:
            with full_symbols_path.open('w') as f:
                json.dump(symbols_data['data']['rows'], f, indent=4)

            symbols = [row['symbol'] for row in symbols_data['data']['rows']]
            if symbols:
                self._save_symbols_list(exchange_dir, exchange, symbols)
                logging.info(f"Saved symbols for {exchange} in {exchange_dir}")
            else:
                logging.warning(f"No symbols found for {exchange} in {exchange_dir}")
        except (OSError, json.JSONDecodeError) as e:
            logging.error(f"Failed to save symbols for {exchange}: {e}")

    def _save_symbols_list(self, directory: Path, filename_prefix: str, symbols: List[str]):
        """
        Save symbols list to JSON and TXT files.

        Args:
            directory (Path): The directory where the files will be saved.
            filename_prefix (str): The prefix to use for the filenames.
            symbols (List[str]): The list of symbols to save.
        """
        try:
            symbols_json_path = directory / f"{filename_prefix}_symbols.json"
            symbols_txt_path = directory / f"{filename_prefix}_symbols.txt"
            with symbols_json_path.open('w') as f:
                json.dump(symbols, f, indent=4)
            with symbols_txt_path.open('w') as f:
                f.write('\n'.join(symbols) + '\n')
        except OSError as e:
            logging.error(f"Failed to save symbols list for {filename_prefix}: {e}")

    def concatenate_and_deduplicate_symbols(self, exchanges: List[str], output_filename: str, open_func: Optional[Callable] = open):
        """
        Concatenate and deduplicate symbols from multiple exchanges.

        Args:
            exchanges (List[str]): The list of exchanges to process.
            output_filename (str): The filename to save the combined symbols to.
            open_func (Optional[Callable]): The function to use for opening files, defaults to the built-in open.
        """
        all_symbols = set()
        for exchange in exchanges:
            symbols_file = self.symbols_dir / exchange / f"{exchange}_symbols.txt"
            if symbols_file.exists():
                try:
                    with symbols_file.open('r') as f:
                        all_symbols.update(symbol.strip() for symbol in f)
                except OSError as e:
                    logging.error(f"Error reading {symbols_file}: {e}")
            else:
                logging.warning(f"File not found: {symbols_file}")

        output_file = self.symbols_dir / 'all' / output_filename
        output_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open_func(output_file, 'w') as f:
                f.write('\n'.join(sorted(all_symbols)) + '\n')
            logging.info(f"Concatenated and deduplicated symbols to {output_file}")
        except OSError as e:
            logging.error(f"Failed to write to {output_file}: {e}")

    def fetch_and_save_all_symbols(self):
        """
        Fetch and save stock symbols for all exchanges, then concatenate and deduplicate symbols.

        Fetches symbols for all exchanges, saves them to their respective directories,
        and then combines and deduplicates the symbols into a single file.
        """
        for exchange in self.exchanges:
            symbols_data = self.fetch_exchange_symbols(exchange)
            if symbols_data:
                self.save_exchange_symbols(symbols_data, exchange)
        self.concatenate_and_deduplicate_symbols(self.exchanges, 'all_symbols.txt')

    def load_exchange_symbols(self, exchange: str) -> Optional[Dict]:
        """
        Load saved stock symbols for a specific exchange.

        Args:
            exchange (str): The exchange to load symbols for (e.g., 'nasdaq').

        Returns:
            Optional[Dict]: The loaded symbols data, or None if the file does not exist or cannot be read.
        """
        file_path = self.symbols_dir / exchange / f"{exchange}_full_symbols.json"
        if file_path.exists():
            try:
                with file_path.open('r') as f:
                    return json.load(f)
            except (OSError, json.JSONDecodeError) as e:
                logging.error(f"Error reading {file_path}: {e}")
                return None
        else:
            logging.warning(f"Data file for {exchange} not found.")
            return None

    def load_all_symbols(self) -> Optional[List[str]]:
        """
        Load concatenated and deduplicated symbols from all exchanges.

        Returns:
            Optional[List[str]]: The list of all concatenated and deduplicated symbols,
                                 or None if the file does not exist or cannot be read.
        """
        output_file = self.symbols_dir / 'all' / 'all_symbols.txt'
        if output_file.exists():
            try:
                with output_file.open('r') as f:
                    return f.read().splitlines()
            except OSError as e:
                logging.error(f"Error reading {output_file}: {e}")
                return None
        else:
            logging.warning(f"Combined data file not found.")
            return None

    def list_exchanges(self) -> List[str]:
        """
        Return the list of exchanges.

        Returns:
            List[str]: The list of exchanges.
        """
        return self.exchanges

    def fetch_fortune500_list(self, url: str = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies') -> pd.DataFrame:
        """
        Fetch the list of Fortune 500 companies from a given URL.

        Args:
            url (str): The URL to fetch the list from.

        Returns:
            pd.DataFrame: The DataFrame containing the list of Fortune 500 companies.
        """
        try:
            html = pd.read_html(url, header=0)
            return html[0]
        except Exception as e:
            logging.error(f"Failed to fetch Fortune 500 list from {url}: {e}")
            return pd.DataFrame()

    def save_fortune500_list(self, df: pd.DataFrame):
        """
        Save the Fortune 500 list to a JSON file.

        Args:
            df (pd.DataFrame): The DataFrame containing the Fortune 500 list.
        """
        file_path = self.fortune500_dir / 'fortune500_list.json'
        try:
            df.to_json(file_path, orient='records', indent=4)
            logging.info(f"Saved Fortune 500 list to {file_path}")
        except OSError as e:
            logging.error(f"Failed to save Fortune 500 list to {file_path}: {e}")

    def load_fortune500_list(self) -> Optional[pd.DataFrame]:
        """
        Load the saved Fortune 500 list from a JSON file.

        Returns:
            Optional[pd.DataFrame]: The DataFrame containing the Fortune 500 list,
                                    or None if the file does not exist or cannot be read.
        """
        file_path = self.fortune500_dir / 'fortune500_list.json'
        if file_path.exists():
            try:
                return pd.read_json(file_path)
            except Exception as e:
                logging.error(f"Error reading Fortune 500 list from {file_path}: {e}")
                return None
        else:
            logging.warning(f"Fortune 500 list file not found.")
            return None

if __name__ == "__main__":
    sf = SymbolFetcher()
    sf.fetch_and_save_all_symbols()
    all_symbols = sf.load_all_symbols()
    if all_symbols:
        print("Fetched and saved symbols for all exchanges:", all_symbols)

    # Fetch, save, and load Fortune 500 list
    fortune500_df = sf.fetch_fortune500_list()
    if not fortune500_df.empty:
        sf.save_fortune500_list(fortune500_df)
        loaded_fortune500_df = sf.load_fortune500_list()
        if loaded_fortune500_df is not None:
            print("Fetched and saved Fortune 500 list:", loaded_fortune500_df)
