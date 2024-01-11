import requests
from datetime import datetime
import time

class Contracts:

    def __init__(self):
        self.list = {}                 # Key = token Contract / Value: Dict: Key = pair Contract, value = Pair obj

    def get_pairs_by_base_token(self, coin_address_list: list, dicQuoteTokens: dict = None, ignore_liq_below: float = 5000, ignore_vol_below: float = 1000):
        base_url = 'https://api.dexscreener.com/latest/dex/tokens/'
        chunk_size = 1  # Maximum items per API call
        # NOTE: there seems to be an issue where not all data is retrieved if our results are too large
        # For common coins like ETH, BNB etc. we have way too many matches.
        # That's why, changed the chunk_size to 1 (from 30)

        for i in range(0, len(coin_address_list), chunk_size):
            chunk = coin_address_list[i:i + chunk_size]

            coins_list = ','.join(chunk)
            url = f'{base_url}{coins_list}'

            response = requests.get(url)
            data = response.json()

            if data['pairs'] is not None:
                for pair_data in data['pairs']:
                    token_contract = pair_data['baseToken']['address'].lower()
                    pair_contract = pair_data['pairAddress'].lower()

                    if dicQuoteTokens is None or pair_data['quoteToken']['address'].lower() in dicQuoteTokens:
                        if 'liquidity' in pair_data and 'volume' in pair_data:
                            if pair_data['liquidity']['usd'] > ignore_liq_below and pair_data['volume']['h24'] > ignore_vol_below:
                                if token_contract not in self.list:
                                    self.list[token_contract] = {}

                                pair = Pair(pair_data)
                                self.list[token_contract][pair_contract] = pair

                print(f'{token_contract} contract retrieved: {len(data["pairs"])} pairs')


            if len(coin_address_list) > 30:
                time.sleep(3)
            elif len(coin_address_list) > 20:
                time.sleep(2)
            else:
                time.sleep(1)


    def get_pairs_by_search(self, dicTokenPairs: dict, ignore_liq_below: float = 5000, ignore_vol_below: float = 1000):
        base_url = 'https://api.dexscreener.com/latest/dex/search?q='
        # dicTokenPairs needs to have %20 between them, the value in the dic doesn't matter

        for tokenPair in dicTokenPairs.keys():
            url = f'{base_url}{tokenPair}'

            response = requests.get(url)
            data = response.json()


            if data['pairs'] is not None:
                for pair_data in data['pairs']:
                    base_contract = pair_data['baseToken']['address'].lower()
                    quote_contract = pair_data['quoteToken']['address'].lower()
                    pair_contract = pair_data['pairAddress'].lower()

                    if f'{base_contract}%20{quote_contract}' in dicTokenPairs:
                        if pair_data['liquidity']['usd'] > ignore_liq_below and pair_data['volume']['h24'] > ignore_vol_below:
                            if base_contract not in self.list:
                                self.list[base_contract] = {}

                            pair = Pair(pair_data)
                            self.list[base_contract][pair_contract] = pair
                            print(pair_contract)

                if len(dicTokenPairs) > 30:
                    time.sleep(3)
                elif len(dicTokenPairs) > 20:
                    time.sleep(2)
                else:
                    time.sleep(1)

class Pair:

    def __init__(self, data):
        self.chain = data.get('chainId')
        self.dexId = data.get('dexId')
        self.pairAddress = data.get('pairAddress', '').lower()
        self.baseToken = data.get('baseToken', {})                      # Address, Name, Symbol
        self.quoteToken = data.get('quoteToken', {})                    # Address, Name, Symbol

        self.priceNative = data.get('priceNative', 0.0)
        self.priceUsd = data.get('priceUsd', 0.0)

        self.tx_counts = data.get('txns', {})                           # m5,h1,h6,h24 / buy,sell
        self.volumes = data.get('volume', {})                           # m5,h1,h6,h24
        self.price_change = data.get('priceChange', {})                 # m5,h1,h6,h24

        liquidity_data = data.get('liquidity', {})
        self.liquidity_usd = liquidity_data.get('usd', 0.0)
        self.fdv = data.get('fdv', 0.0)
        self.createdAt = data.get('pairCreatedAt', 0.0) / 1000          # Convert to seconds

        current_time_utc = datetime.utcnow()
        created_time_utc = datetime.utcfromtimestamp(self.createdAt)
        self.age_in_days = (current_time_utc - created_time_utc).days

        self.vol_to_tvl = self.volumes.get('h24', 0.0) / self.liquidity_usd