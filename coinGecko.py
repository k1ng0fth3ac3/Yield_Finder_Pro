import requests


class Gecko:

    coin_ids_url = 'https://api.coingecko.com/api/v3/coins/list?include_platform=true'


    def __init__(self):
        self.tokens = {}
        self.contracts = {}

    def get_token_ids(self):

        response = requests.get(self.coin_ids_url)
        if response.status_code == 200:
            data = response.json()

            for coin_data in data:
                coin_info = Coin_info(coin_data)
                self.tokens[coin_info.id] = coin_info
                self.tokens[coin_info.id] = coin_info

                for chain, contract in coin_info.chains.items():
                    contract_info = Contract_info(coin_info.id, chain,coin_info.symbol, coin_info.name)
                    self.contracts[contract.lower()] = contract_info

        else:
            print("Could not access the token_ids API point, sucks!")
            return


class Coin_info:

    def __init__(self, data):
        self.id: str
        self.symbol: str
        self.name: str
        self.chains = []

        self.parse(data)

    def parse(self, data):
        self.id = data.get('id')
        self.symbol = data.get('symbol')
        self.name = data.get('name')
        self.chains = data.get('platforms',{})


class Contract_info:

    def __init__(self, id, chain, symbol, name):
        self.id: str = id
        self.chain: str = chain
        self.symbol: str = symbol
        self.name: str = name
