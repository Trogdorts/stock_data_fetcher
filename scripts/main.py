from stock_data_fetcher.symbol_fetcher import SymbolFetcher

# Initialize the SymbolFetcher instance
sf = SymbolFetcher()

all_symbols = sf.load_all_symbols()

print(all_symbols)