from stock_data_fetcher.symbol_fetcher import SymbolFetcher

# Initialize the SymbolFetcher instance
sf = SymbolFetcher()

# Example: Fetch symbols for a specific exchange (e.g., 'nasdaq')
nasdaq_symbols_data = sf.fetch_exchange_symbols('nasdaq')
print("NASDAQ Symbols Data:", nasdaq_symbols_data)

# Example: Save symbols for a specific exchange (e.g., 'nasdaq')
if nasdaq_symbols_data:
    sf.save_exchange_symbols(nasdaq_symbols_data, 'nasdaq')

# Example: Load saved symbols for a specific exchange (e.g., 'nasdaq')
nasdaq_symbols = sf.load_exchange_symbols('nasdaq')
print("Loaded NASDAQ Symbols:", nasdaq_symbols)

# Example: Fetch and save symbols for all exchanges
sf.fetch_and_save_all_symbols()

# Example: Load concatenated and deduplicated symbols from all exchanges
all_symbols = sf.load_all_symbols()
print("All Symbols:", all_symbols)

# Example: List available exchanges
exchanges = sf.list_exchanges()
print("Available Exchanges:", exchanges)

# Example: Manually concatenate and deduplicate symbols from specific exchanges and save to a custom file
sf.concatenate_and_deduplicate_symbols(['nasdaq', 'nyse'], 'custom_combined_symbols.txt')

# Example: Load the custom combined symbols
custom_combined_symbols = sf.load_all_symbols()
print("Custom Combined Symbols:", custom_combined_symbols)

# Full Example Workflow
print("\n--- Full Example Workflow ---")

# Initialize the SymbolFetcher instance
sf = SymbolFetcher()

# Fetch and save symbols for all exchanges
sf.fetch_and_save_all_symbols()

# Load and print symbols for each exchange
print("NASDAQ Symbols:", sf.load_exchange_symbols('nasdaq'))
print("NYSE Symbols:", sf.load_exchange_symbols('nyse'))
print("AMEX Symbols:", sf.load_exchange_symbols('amex'))

# Load and print all concatenated and deduplicated symbols
print("All Symbols:", sf.load_all_symbols())

# List available exchanges
print("Available Exchanges:", sf.list_exchanges())

# Manually concatenate and deduplicate symbols from specific exchanges and save to a custom file
sf.concatenate_and_deduplicate_symbols(['nasdaq', 'nyse'], 'custom_combined_symbols.txt')

# Load and print custom combined symbols
custom_combined_symbols = sf.load_all_symbols()
print("Custom Combined Symbols:", custom_combined_symbols)
