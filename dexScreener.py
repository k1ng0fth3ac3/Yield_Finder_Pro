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

    def __init__(self,data):
        self.chain: str = data['chainId']
        self.dexId: str = data['dexId']
        self.pairAddress: str = data['pairAddress'].lower()
        self.baseToken: dict = data['baseToken']                # Address, Name, Symbol
        self.quoteToken: dict = data['quoteToken']              # Address, Name, Symbol

        self.priceNative: float = data['priceNative']
        self.priceUsd: float = data['priceUsd']

        self.tx_counts: dict = data['txns']                     # m5,h1,h6,h24 / buy,sell
        self.volumes: dict = data['volume']                     # m5,h1,h6,h24
        self.price_change: dict = data['priceChange']           # m5,h1,h6,h24

        self.liquidity_usd: float = data['liquidity']['usd']
        self.fdv: float = data['fdv']
        self.createdAt: float = data['pairCreatedAt'] / 1000    # Convert to seconds
        self.age_in_days: int = (datetime.utcnow() - datetime.utcfromtimestamp(self.createdAt)).days


        self.vol_to_tvl = self.volumes['h24'] / self.liquidity_usd