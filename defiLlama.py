import requests
from datetime import datetime, timedelta
import csv
import os
import json


class Pools:
    url = 'https://yields.llama.fi/pools'


    def __init__(self):
        self.list = {}                      # Pool id / pool
        self.list_by_chain = {}             # Chain / pool
        self.list_by_project = {}           # Project / pool
        self.list_by_pair = {}              # Pair / pool
        self.list_by_token = {}             # Token / pool
        self.list_by_contract = {}          # Contract / pool

        self.tokens = {}                    # Token / Contract
        self.tokens_by_chain = {}           # Token / Dict(Chain / Contract)
        self.contracts = {}                 # Contract / Token

        self.csv_pools_data_path = 'pools_data.csv'

    def get_all_pools(self, chains: list = None, getFreshData: bool = False):

        if getFreshData:
            fetch_from_api = True
        else:
            if not os.path.exists(self.csv_pools_data_path):
                fetch_from_api = True
            else:
                last_modified_time = datetime.fromtimestamp(os.path.getmtime(self.csv_pools_data_path))
                current_time = datetime.now()
                if current_time - last_modified_time > timedelta(hours=6):
                    fetch_from_api = True
                else:
                    fetch_from_api = False


        if fetch_from_api:
            response = requests.get(self.url)
            if response.status_code == 200:
                data = response.json()
                with open(self.csv_pools_data_path, 'w', newline='', encoding='utf-8') as file:
                    json.dump(data, file, ensure_ascii=False)

            else:
                print("Could not access the API point, sucks!")
                return
        else:
            with open(self.csv_pools_data_path, 'r', encoding='utf-8') as file:
                data = json.loads(file.read())

        for pool_data in data['data']:
            if chains is None or pool_data['chain'].lower() in chains:
                pool = Pool(pool_data)
                self.list[pool_data['pool']] = pool

    def group_by_chain(self):
        for pool in self.list.values():
            if pool.chain not in self.list_by_chain:
                self.list_by_chain[pool.chain] = {}
            self.list_by_chain[pool.chain][pool.pool] = pool

    def group_by_project(self):
        for pool in self.list.values():
            if pool.project not in self.list_by_project:
                self.list_by_project[pool.project] = {}
            self.list_by_project[pool.project][pool.pool] = pool

    def group_by_pair(self):
        for pool in self.list.values():
            if pool.symbol not in self.list_by_pair:
                self.list_by_pair[pool.symbol] = {}
            self.list_by_pair[pool.symbol][pool.pool] = pool

    def group_by_token(self):
        for pool in self.list.values():
            if '-' in pool.symbol:
                for i in range(0,pool.symbol.count('-')+1):
                    token = pool.symbol.split('-')[i]
                    if token not in self.list_by_token:
                        self.list_by_token[token] = {}
                    self.list_by_token[token][pool.pool] = pool
            else:
                if pool.symbol not in self.list_by_token:
                    self.list_by_token[pool.symbol] = {}
                self.list_by_token[pool.symbol][pool.pool] = pool

    def group_by_contract(self):
        for pool in self.list.values():
            if pool.underlyingTokens is not None:
                for address in pool.underlyingTokens:
                    if address not in self.list_by_contract:
                        self.list_by_contract[address] = {}
                    self.list_by_contract[address][pool.pool] = pool
            else:
                if '' not in self.list_by_contract:
                    self.list_by_contract[''] = {}
                self.list_by_contract[''][pool.pool] = pool

    def group_by_custom(self, grBy1, grBy2=None, grBy3=None, grBy4=None, grBy5=None):
        pass
        # Count number of not None
        # Create dic like this: custom_list[key1][key2][key3][key4][key5]

    def match_contracts_to_tokens(self):

        if len(self.list_by_token) == 0:
            self.group_by_token()
        if len(self.list_by_pair) == 0:
            self.group_by_pair()

        for token, token_pools in self.list_by_token.items():
            prospects = {}
            for token_pool in token_pools.values():
                pair_pools = self.list_by_pair[token_pool.symbol]
                for pair, pair_pool in pair_pools.items():

                    if pair_pool.underlyingTokens is not None:
                        for address in pair_pool.underlyingTokens:
                            if pair_pool.chain.lower() + '_' + address not in prospects:
                                if address not in self.contracts:
                                    prospects[pair_pool.chain.lower() + '_' + address] = pair_pool.symbol
                            elif prospects[pair_pool.chain.lower() + '_' + address] != pair_pool.symbol:
                                self.tokens[pair_pool.chain.lower() + '_' + token] = address
                                self.contracts[address] = pair_pool.chain.lower() + '_' + token

        # Need another round for tokens that have only one single pair
        for token, token_pools in self.list_by_token.items():
            for token_pool in token_pools.values():
                pair_pools = self.list_by_pair[token_pool.symbol]
                for pair, pair_pool in pair_pools.items():
                    if pair_pool.chain.lower() + '_' + token not in self.tokens:        # Missing items
                        if pair_pool.symbol.count('-') ==1:                             # We can do this only for pairs, not triplets
                            if pair_pool.underlyingTokens is not None:
                                for address in pair_pool.underlyingTokens:
                                    if address not in self.contracts:                   # The other address has been assigned, this one not
                                        self.tokens[pair_pool.chain.lower() + '_' + token] = address
                                        self.contracts[address] = pair_pool.chain.lower() + '_' + token

    def get_token_contracts_by_chain(self):
        if len(self.tokens) == 0:
            self.match_contracts_to_tokens()

        for chain_token, contract in self.tokens.items():
            chain = chain_token.split('_')[0]
            token = chain_token.split('_')[1]

            if token not in self.tokens_by_chain:
                self.tokens_by_chain[token] = {}
            self.tokens_by_chain[token][chain] = contract



class Pool:

    def __init__(self, data):
        self.chain = None
        self.project = None
        self.symbol = None
        self.tvlUsd = None
        self.apyBase = None
        self.apyReward = None
        self.apy = None
        self.rewardTokens = None
        self.pool = None
        self.apyPct1D = None
        self.apyPct7D = None
        self.apyPct30D = None
        self.stablecoin = None
        self.ilRisk = None
        self.exposure = None
        self.predictions = None
        self.poolMeta = None
        self.mu = None
        self.sigma = None
        self.count = None
        self.outlier = None
        self.underlyingTokens = None
        self.il7d = None
        self.apyBase7d = None
        self.apyMean30d = None
        self.volumeUsd1d = None
        self.volumeUsd7d = None
        self.apyBaseInception = None


        self.volume_to_tvl: float = None                # Volume / TVL
        self.apy_1d: float = None                       # apy / 365

        self.historical_tvl = {}                        # Key: Date / TVL
        self.historical_apy = {}                        # Key: Date / apy
        self.tvl_ath: float = None                      # TVL ATH value in USD
        self.tvl_from_ath: float = None                 # % decline in TVL from ATH
        self.tvl_days_from_ath: int = None              # Days since we had ATH in TVL
        self.apy_ath: float = None                      # APY ATH
        self.apy_from_ath: float = None                 # % decline in APY from ATH (from 50% to 25% = 50% decline)
        self.apy_days_from_ath: int = None              # Days since we had ATH in APY


        self.parse(data)

    def parse(self, data):
        self.chain = data['chain'].lower()
        self.project = data['project']
        self.symbol = data['symbol']
        self.tvlUsd = data['tvlUsd']
        self.apyBase = data['apyBase']
        self.apyReward = data['apyReward']
        self.apy = data['apy']
        self.rewardTokens = data['rewardTokens']
        self.pool = data['pool']
        self.apyPct1D = data['apyPct1D']
        self.apyPct7D = data['apyPct7D']
        self.apyPct30D = data['apyPct30D']
        self.stablecoin = data['stablecoin']
        self.ilRisk = data['ilRisk']
        self.exposure = data['exposure']
        self.predictions = data['predictions']
        self.poolMeta = data['poolMeta']
        self.mu = data['mu']
        self.sigma = data['sigma']
        self.count = data['count']
        self.outlier = data['outlier']
        self.underlyingTokens = data['underlyingTokens']
        self.il7d = data['il7d']
        self.apyBase7d = data['apyBase7d']
        self.apyMean30d = data['apyMean30d']
        self.volumeUsd1d = data['volumeUsd1d']
        self.volumeUsd7d = data['volumeUsd7d']
        self.apyBaseInception = data['apyBaseInception']



    def get_history(self, limit_days: int = 365):
        # Care with the rate limitation. Only get the history if we are really interested in the Pool
        url = 'https://yields.llama.fi/chart/' + self.pool

        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()

            history_ = data['data']
            history_.reverse()              # From newest to oldest

            for day, snapshot in enumerate(history_, start=1):
                timestamp = datetime.fromisoformat(snapshot['timestamp'][:-1])
                timestamp.strftime("%Y-%m-%d")

                tvl = float(snapshot['tvlUsd'])
                apy = float(snapshot['apy'])

                self.historical_tvl[timestamp] = tvl
                self.historical_apy[timestamp] = apy

                if self.tvl_ath is None or self.tvl_ath < tvl:
                    self.tvl_ath = tvl
                    self.tvl_from_ath = (tvl - self.tvlUsd) / tvl
                    self.tvl_days_from_ath = day

                if self.apy_ath is None or self.apy_ath < apy:
                    self.apy_ath = apy
                    self.apy_from_ath = (apy - self.apy) / apy
                    self.apy_days_from_ath = day

                if day == limit_days:
                    break
        else:
            print(f'Issue getting History for {self.pool} - {self.project} - {self.symbol}')



class Protocols:
    url_protocols = 'https://api.llama.fi/protocols'
    url_dexes = 'https://api.llama.fi/overview/dexs?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyVolume'
    url_fees = 'https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyFees'

    def __init__(self):
        self.all_protocols = {}     # Protocol Name / ProtocolObj
        self.dexes = {}             # Protocol Id / DexObj
        self.fees = {}              # Protocol Id / FeesObj
        self.chains = {}            # Chain Name / ChainObj


    def get_all_data(self):

        self.get_all_protocols()
        self.get_all_dexes()
        self.get_fees()

        for protocol in self.all_protocols.values():

            if protocol.id in self.dexes:
                protocol.dex = self.dexes[protocol.id]

            if protocol.id in self.fees:
                protocol.fees = self.fees[protocol.id]



    def get_all_protocols(self):

        response = requests.get(self.url_protocols)
        if response.status_code == 200:
            data = response.json()

            for project_data in data:
                protocol = Protocol(project_data)
                self.all_protocols[project_data['name']] = protocol


        else:
            print("Could not access the API point for Protocols, sucks!")
            return

    def get_all_dexes(self):

        response = requests.get(self.url_dexes)
        if response.status_code == 200:
            data = response.json()

            dexes_data = data['protocols']

            for dex_data in dexes_data:
                dex = Dex(dex_data)
                self.dexes[dex_data['defillamaId']] = dex

        else:
            print("Could not access the API point for Dexes, sucks!")
            return

    def get_fees(self):
        response = requests.get(self.url_fees)
        if response.status_code == 200:
            data = response.json()

            fees_data = data['protocols']

            for fee_data in fees_data:
                protocol_fees = Protocol_Fees(fee_data)
                self.fees[fee_data['defillamaId']] = protocol_fees

        else:
            print("Could not access the API point for Fees, sucks!")
            return



class Protocol:

    chain_fix = {}
    chain_fix['binance'] = 'bsc'
    chain_fix['xdai'] = 'gnosis'
    chain_fix['op_bnb'] = 'opbnb'

    excl_chains_containing = ['staking','pool','borrowed','vesting','offers','treasury']

    def __init__(self, data):
        self.id: int
        self.name: str
        self.symbol: str
        self.url: str
        self.chain: str
        self.logo: str
        self.gecko_id: str
        self.cmc_id: int
        self.category: str
        self.twitter: str
        self.listedAt: float
        self.slug: str
        self.tvl: float
        self.mcap: float

        self.chain_tvl = {}                 # Key: Chain / Value: tvl

        self.dex: Dex                       # Only when Dex object exists
        self.fees: Protocol_Fees            # Only when Fees object exists

        self.parse(data)

    def parse(self, data):
        self.id = data.get('id')
        self.name = data.get('name')
        self.symbol = data.get('symbol')
        self.url = data.get('url')
        self.chain = data.get('chain')
        self.logo = data.get('logo')
        self.gecko_id = data.get('gecko_id', None)
        self.cmc_id = data.get('cmcId', None)
        self.category = data.get('category', None)
        self.twitter = data.get('twitter', None)
        self.listedAt = data.get('listedAt', None)
        self.slug = data.get('slug')
        self.tvl = data.get('tvl')
        self.mcap = data.get('mcap')

        dicChainTvls = data['chainTvls']
        for chain, tvl in dicChainTvls.items():
            chain_name = chain.lower()

            real_chain = True
            for exclusion in self.excl_chains_containing:
                if exclusion in chain_name:
                    real_chain = False

            if real_chain:
                if chain_name in self.chain_fix:
                    chain_name = self.chain_fix[chain_name]
                self.chain_tvl[chain_name] = tvl


class Dex:

    def __init__(self, data):
        self.id: int
        self.name: str
        self.displayName: str
        self.module: str
        self.category: str
        self.logo: str
        self.volume: float
        self.chain_volume = {}

        self.parse(data)

    def parse(self, data):
        self.id = data.get('defillamaId')
        self.name = data.get('name')
        self.displayName = data.get('displayName')
        self.module = data.get('module')
        self.category = data.get('category')
        self.logo = data.get('logo')
        self.volume = data.get('dailyVolume')


        dicChainVolumes = data['breakdown24h']
        chains = data['chains']
        if dicChainVolumes is not None:
            for i, chain in enumerate(chains):
                if i < len(dicChainVolumes):
                    volume_data = dicChainVolumes[list(dicChainVolumes.keys())[i]]
                    volume = next(iter(volume_data.values()))
                    self.chain_volume[chain.lower()] = volume


class Protocol_Fees:

    def __init__(self, data):
        self.id: str
        self.name: str
        self.displayName: str
        self.fees: float

        self.revenue: float
        self.userFees: float
        self.holdersRevenue: float
        self.creatorRevenue: float
        self.supplySideRevenue: float
        self.protocolRevenue: float
        self.bribesRevenue: float
        self.tokenTaxes: float

        self.parse(data)

    def parse(self,data):
        self.id = data.get('defillamaId')
        self.name = data.get('name')
        self.displayName = data.get('displayName')

        self.fees = data.get('total24h',0)

        self.revenue = data.get('dailyRevenue',0)
        self.userFees = data.get('dailyUserFees',0)
        self.holdersRevenue = data.get('dailyHoldersRevenue',0)
        self.creatorRevenue = data.get('dailyCreatorRevenue',0)
        self.supplySideRevenue = data.get('dailySupplySideRevenue',0)
        self.protocolRevenue = data.get('dailyProtocolRevenue',0)
        self.bribesRevenue = data.get('dailyBribesRevenue',0)
        self.tokenTaxes = data.get('dailyTokenTaxes',0)

        self.chain_fees = {}

        dicChainFees = data['breakdown24h']
        chains = data['chains']
        if not dicChainFees is None:
            for i, chain in enumerate(chains):
                if i < len(dicChainFees):
                    fees_data = dicChainFees[list(dicChainFees.keys())[i]]
                    fees = next(iter(fees_data.values()))
                    self.chain_fees[chain.lower()] = fees


class Chains:
    url_chains = 'https://api.llama.fi/v2/chains'
    url_stablecoins = 'https://stablecoins.llama.fi/stablecoinchains'

    def __init__(self):
        self.list = {}                      # Key: Chain name / Value: Chain Obj
        self.volumes = {}                   # Key: Chain name / Value: Volume
        self.fees = {}                      # Key: Chain name / Value: Fees
        self.protocol_counts = {}           # Key: Chain name / Value: Protocol count
        self.pool_counts = {}               # Key: Chain name / Value: Pool count
        self.stables_circulating = {}       # Key: Chain name / Value: stable TVL

    def get_chains(self, objProtocols: Protocols = None, objPools: Pools = None):
        response = requests.get(self.url_chains)
        if response.status_code == 200:
            data = response.json()

            self.get_chain_volume_and_fees(objProtocols)
            self.get_chain_protocol_counts(objProtocols)
            self.get_chain_pool_counts(objPools)
            self.get_chain_stablecoins(objProtocols)


            for chain_data in data:
                chain_name = chain_data['name'].lower()

                if chain_name in self.volumes:
                    volume = self.volumes[chain_name]
                else:
                    volume = None
                if chain_name in self.fees:
                    fees = self.fees[chain_name]
                else:
                    fees = None
                if chain_name in self.pool_counts:
                    pool_count = self.pool_counts[chain_name]
                else:
                    pool_count = 0
                if chain_name in self.protocol_counts:
                    protocol_count = self.protocol_counts[chain_name]
                else:
                    protocol_count = 0
                if chain_name in self.stables_circulating:
                    stables_circulating = self.stables_circulating[chain_name]
                else:
                    stables_circulating = None


                chain = Chain(chain_data, volume, fees, protocol_count, pool_count, stables_circulating)
                self.list[chain_name] = chain
        else:
            print("Could not access the API point for Chains, sucks!")
            return


    def get_chain_volume_and_fees(self, objProtocols: Protocols = None):

        if objProtocols is None:
            objProtocols = Protocols()

        if len(objProtocols.all_protocols) == 0:
            objProtocols.get_all_data()


        for protocol in objProtocols.all_protocols.values():

            # ----- Volume from Dex
            if hasattr(protocol, 'dex'):
                chain_volume = protocol.dex.chain_volume

                for chain, volume in chain_volume.items():
                    self.volumes.setdefault(chain,0)
                    self.volumes[chain] += volume

            # -----/
            # ----- Fees
            if hasattr(protocol, 'fees'):
                chain_fees = protocol.fees.chain_fees
                for chain, fees in chain_fees.items():
                    self.fees.setdefault(chain,0)
                    self.fees[chain] += fees

            # -----/

    def get_chain_protocol_counts(self, objProtocols: Protocols = None):
        if objProtocols is None:
            objProtocols = Protocols()
        if len(objProtocols.all_protocols) == 0:
            objProtocols.get_all_data()

        for protocol in objProtocols.all_protocols.values():
            for chain in protocol.chain_tvl.keys():
                self.protocol_counts.setdefault(chain, 0)
                self.protocol_counts[chain] += 1

    def get_chain_pool_counts(self, objPools: Pools = None):
        if objPools is None:
            objPools = Pools()
        if len(objPools.list) == 0:
            objPools.get_all_pools()

        for pool in objPools.list.values():
            self.pool_counts.setdefault(pool.chain, 0)
            self.pool_counts[pool.chain] += 1

    def get_chain_stablecoins(self, objProtocols: Protocols = None):
        response = requests.get(self.url_stablecoins)
        if response.status_code == 200:
            data = response.json()

            for chainData in data:
                chain_name = chainData['name'].lower()
                circulatingUsd = sum(chainData.get("totalCirculatingUSD", {}).values())
                self.stables_circulating[chain_name] = circulatingUsd

        else:
            print("Could not access the API point for Stable coins, sucks!")
            return


class Chain:

    def __init__(self, data, volume, fees, protocols, pools, stables_circulating):
        self.name: str
        self.gecko_id: str
        self.cmc_id: str
        self.token_symbol: str
        self.chain_id: int
        self.tvl: float

        self.fees: float = fees                 # Take from Fees objects (Might not be available for all)
        self.volume: float = volume             # Take from Dex objects (Might not be available for all)
        self.protocolsC: int = protocols
        self.poolsC: int = pools
        self.stables_circulating: float = stables_circulating

        self.parse(data)

    def parse(self,data):
        self.name = data.get('name').lower()
        self.gecko_id = data.get('gecko_id')
        self.cmc_id = data.get('cmcId')
        self.token_symbol = data.get('tokenSymbol')
        self.chain_id = data.get('chainId')
        self.tvl = data.get('tvl',0)



