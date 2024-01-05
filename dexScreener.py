import requests
from datetime import datetime

class Contracts:

    def __init__(self):
        self.list = {}                 # Key = token Contract / Value: Dict: Key = pair Contract, value = Pair obj

    def get_pairs(self, coin_address_list: list, dicQuoteTokens: dict = None, ignore_liq_below: float = 5000, ignore_vol_below: float = 1000):
        base_url = 'https://api.dexscreener.com/latest/dex/tokens/'
        chunk_size = 30  # Maximum items per API call

        for i in range(0, len(coin_address_list), chunk_size):
            chunk = coin_address_list[i:i + chunk_size]

            coins_list = ','.join(chunk)
            url = f'{base_url}{coins_list}'

            response = requests.get(url)
            data = response.json()

            for pair_data in data['pairs']:
                token_contract = pair_data['baseToken']['address'].lower()
                pair_contract = pair_data['pairAddress'].lower()

                if dicQuoteTokens is None or pair_data['quoteToken']['address'].lower() in dicQuoteTokens:
                    if pair_data['liquidity']['usd'] > ignore_liq_below and pair_data['volume']['h24'] > ignore_vol_below:
                        if token_contract not in self.list:
                            self.list[token_contract] = {}

                        pair = Pair(pair_data)
                        self.list[token_contract][pair_contract] = pair

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