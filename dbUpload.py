import datetime
from dbTables import Tables_info
from dbManager import Connection
from datetime import date
from defiLlama import Pools, Protocols,Protocol, Chains
from coinGecko import Gecko
import pandas as pd

class Upload:

    def __init__(self):
        self.action = 'Data upload'
        self.tables_info = Tables_info()

        self.pools = Pools()
        self.protocols = Protocols()
        self.chains = Chains()


    def pools_history(self, overWriteTheDay: bool = False):

        # We will only take pools that have had at least 20% APY (at any point)


        # ----- Get Table info
        table_name = 'pools_history'
        connection = Connection()
        dicTableInfo = connection.get_table_info(table_name)
        # -----/

        if dicTableInfo['upload_last_date'] is None or \
                (dicTableInfo['upload_last_date'] != datetime.datetime.now().date() or \
                        overWriteTheDay):

            if dicTableInfo['upload_last_date'] == datetime.datetime.now().date():
                connection.delete_day_from_table(table_name)
                connection.delete_log_entry(table_name,'Data upload')

            if len(self.pools.list) == 0:
                self.pools.get_all_pools(getFreshData=True)

            dicPoolIds = connection.get_uniq_values_from_col('pools_info','pool_id','id')

            data = []
            for pool in self.pools.list.values():

                # ----- Reward tokens
                if pool.rewardTokens is None or len(pool.rewardTokens) == 0:
                    reward_token_1 = None
                    reward_token_2 = None
                elif len(pool.rewardTokens) == 1:
                    reward_token_1 = pool.rewardTokens[0]
                    reward_token_2 = None
                else:
                    reward_token_1 = pool.rewardTokens[0]
                    reward_token_2 = pool.rewardTokens[1]
                # ------/

                # ------ Pool Underlying Tokens
                pool_tokens = {}
                pool_tokens[0] = None
                pool_tokens[1] = None
                pool_tokens[2] = None
                if not pool.underlyingTokens is None:
                    for i in range(0,len(pool.underlyingTokens)):
                        pool_tokens[i] = pool.underlyingTokens[i]
                # ------/

                if pool.pool in dicPoolIds:
                    pool_id = dicPoolIds[pool.pool]     # If pool not in pools_info, the APY is below required

                    data_row = ()
                    data_row = (f'{datetime.datetime.now().date()}',
                                pool.tvlUsd,
                                pool.apyBase,
                                pool.apyReward,
                                pool.apy,
                                pool.volumeUsd1d,
                                pool_id)

                    data.append(data_row)

            columns = dicTableInfo['columns']

            connection.insert_to_table(table_name,columns,data)
            connection.add_to_action_log(table_name,self.action,len(data),'-')
            connection.close_connection()

        else:
            print(f'{table_name} already uploaded for {datetime.datetime.now().date()}')
            connection.close_connection()


    def protocols_history(self, overWriteTheDay: bool = False):

        # ----- Get Table info
        table_name = 'protocols_history'
        connection = Connection()
        dicTableInfo = connection.get_table_info(table_name)
        # -----/

        if dicTableInfo['upload_last_date'] is None or \
                (dicTableInfo['upload_last_date'] != datetime.datetime.now().date() or \
                        overWriteTheDay):

            if dicTableInfo['upload_last_date'] == datetime.datetime.now().date():
                connection.delete_day_from_table(table_name)
                connection.delete_log_entry(table_name,'Data upload')


            if len(self.protocols.all_protocols) == 0:
                self.protocols.get_all_data()

            data = []
            for protocol in self.protocols.all_protocols.values():

                # ----- Volume from Dex
                if hasattr(protocol, 'dex'):
                    dex_volume = protocol.dex.volume
                else:
                    dex_volume = None
                # -----/
                # ----- Fees
                if hasattr(protocol, 'fees'):
                    fees = protocol.fees.fees
                    revenue = protocol.fees.revenue
                    user_fees = protocol.fees.userFees
                    holders_revenue = protocol.fees.holdersRevenue
                    creator_revenue = protocol.fees.creatorRevenue
                    supply_side_revenue = protocol.fees.supplySideRevenue
                    protocol_revenue = protocol.fees.protocolRevenue
                else:
                    fees = None
                    revenue = None
                    user_fees = None
                    holders_revenue = None
                    creator_revenue = None
                    supply_side_revenue = None
                    protocol_revenue = None
                # -----/


                data_row = ()
                data_row = (f'{protocol.id}',
                            f'{datetime.datetime.now().date()}',
                            protocol.tvl,
                            dex_volume,
                            fees,
                            revenue,
                            user_fees,
                            holders_revenue,
                            creator_revenue,
                            supply_side_revenue,
                            protocol_revenue
                            )

                data.append(data_row)

            columns = dicTableInfo['columns']
            connection.insert_to_table(table_name, columns, data)
            connection.add_to_action_log(table_name, self.action, len(data), '-')
            connection.close_connection()

        else:
            print(f'{table_name} already uploaded for {datetime.datetime.now().date()}')
            connection.close_connection()


    def protocols_chains_history(self, overWriteTheDay: bool = False):

        # ----- Get Table info
        table_name = 'protocols_chains_history'
        connection = Connection()
        dicTableInfo = connection.get_table_info(table_name)
        # -----/

        if dicTableInfo['upload_last_date'] is None or \
                (dicTableInfo['upload_last_date'] != datetime.datetime.now().date() or \
                        overWriteTheDay):

            if dicTableInfo['upload_last_date'] == datetime.datetime.now().date():
                connection.delete_day_from_table(table_name)
                connection.delete_log_entry(table_name,'Data upload')


            if len(self.protocols.all_protocols) == 0:
                self.protocols.get_all_data()

            data = []
            for protocol in self.protocols.all_protocols.values():

                # ----- Volume from Dex
                if hasattr(protocol, 'dex'):
                    chain_volume = protocol.dex.chain_volume
                else:
                    chain_volume = None
                    volume = None
                # -----/
                # ----- Fees
                if hasattr(protocol, 'fees'):
                    chain_fees = protocol.fees.chain_fees
                else:
                    chain_fees = None
                    fees = None
                # -----/
                # ----- TVL
                for chain, tvl in protocol.chain_tvl.items():
                    if not chain_volume is None:
                        if chain in chain_volume:
                            volume = chain_volume[chain]
                        else:
                            volume = None
                    else:
                        volume = None

                    if not chain_fees is None:
                        if chain in chain_fees:
                            fees = chain_fees[chain]
                        else:
                            fees = None
                    else:
                        fees = None


                    data_row = ()
                    data_row = (f'{chain}',
                                f'{datetime.datetime.now().date()}',
                                f'{protocol.id}',
                                tvl,
                                volume,
                                fees
                                )

                    data.append(data_row)
                # -----/

            columns = dicTableInfo['columns']
            connection.insert_to_table(table_name, columns, data)
            connection.add_to_action_log(table_name, self.action, len(data), '-')
            connection.close_connection()

        else:
            print(f'{table_name} already uploaded for {datetime.datetime.now().date()}')
            connection.close_connection()

    def chains_history(self, overWriteTheDay: bool = False):

        # ----- Get Table info
        table_name = 'chains_history'
        connection = Connection()
        dicTableInfo = connection.get_table_info(table_name)
        # -----/

        if dicTableInfo['upload_last_date'] is None or \
                (dicTableInfo['upload_last_date'] != datetime.datetime.now().date() or \
                 overWriteTheDay):

            if dicTableInfo['upload_last_date'] == datetime.datetime.now().date():
                connection.delete_day_from_table(table_name)
                connection.delete_log_entry(table_name, 'Data upload')

            if len(self.chains.list) == 0:

                if len(self.protocols.all_protocols) == 0:
                    self.protocols.get_all_data()
                if len(self.pools.list) == 0:
                    self.pools.get_all_pools()

                self.chains.get_chains(objPools=self.pools,objProtocols=self.protocols)

            data = []
            for chain in self.chains.list.values():

                    data_row = ()
                    data_row = (f'{chain.name}',
                                f'{datetime.datetime.now().date()}',
                                chain.tvl,
                                chain.volume,
                                chain.fees,
                                chain.protocolsC,
                                chain.poolsC,
                                chain.stables_circulating
                                )

                    data.append(data_row)
                # -----/

            columns = dicTableInfo['columns']
            connection.insert_to_table(table_name, columns, data)
            connection.add_to_action_log(table_name, self.action, len(data), '-')
            connection.close_connection()

        else:
            print(f'{table_name} already uploaded for {datetime.datetime.now().date()}')
            connection.close_connection()

    def chains_info(self):

        # ----- Get Table info
        table_name = 'chains_info'
        connection = Connection()
        dicTableInfo = connection.get_table_info(table_name)
        # -----/

        connection.clear_whole_table(table_name)

        if len(self.chains.list) == 0:
            if len(self.protocols.all_protocols) == 0:
                self.protocols.get_all_data()
            if len(self.pools.list) == 0:
                self.pools.get_all_pools()

            self.chains.get_chains(objPools=self.pools,objProtocols=self.protocols)

        data = []
        for chain in self.chains.list.values():

                data_row = ()
                data_row = (f'{chain.name}',
                            int(chain.chain_id) if (chain.chain_id is not None and int(chain.chain_id) < 2147483647) else None,
                            f'{chain.gecko_id}',
                            chain.cmc_id,
                            f'{chain.token_symbol}'
                            )

                data.append(data_row)
            # -----/

        columns = dicTableInfo['columns']
        connection.insert_to_table(table_name, columns, data)
        connection.add_to_action_log(table_name, self.action, len(data), 'Update')
        connection.close_connection()

    def protocols_info(self):

        # ----- Get Table info
        table_name = 'protocols_info'
        connection = Connection()
        dicTableInfo = connection.get_table_info(table_name)
        # -----/

        connection.clear_whole_table(table_name)

        if len(self.chains.list) == 0:
            if len(self.protocols.all_protocols) == 0:
                self.protocols.get_all_data()
            if len(self.pools.list) == 0:
                self.pools.get_all_pools()


        data = []
        for protocol in self.protocols.all_protocols.values():
            data_row = ()
            data_row = (f'{protocol.id}',
                        f'{protocol.name}',
                        f'{protocol.chain}',
                        f'{protocol.symbol}',
                        f'{protocol.url}',
                        protocol.listedAt,
                        f'{protocol.gecko_id}' if not protocol.gecko_id is None else None,
                        f'{protocol.twitter}',
                        f'{protocol.category}',
                        f'{protocol.slug}'
                        )

            data.append(data_row)
        # -----/

        columns = dicTableInfo['columns']
        connection.insert_to_table(table_name, columns, data)
        connection.add_to_action_log(table_name, self.action, len(data), 'Update')
        connection.close_connection()

    def categories_history(self, overWriteTheDay: bool = False):

        # ----- Get Table info
        table_name = 'categories_history'
        connection = Connection()
        dicTableInfo = connection.get_table_info(table_name)
        # -----/

        if dicTableInfo['upload_last_date'] is None or \
                (dicTableInfo['upload_last_date'] != datetime.datetime.now().date() or \
                 overWriteTheDay):

            if dicTableInfo['upload_last_date'] == datetime.datetime.now().date():
                connection.delete_day_from_table(table_name)
                connection.delete_log_entry(table_name, 'Data upload')

            if len(self.protocols.all_protocols) == 0:
                self.protocols.get_all_data()


            dicCategories = {}
            for protocol in self.protocols.all_protocols.values():
                if not protocol.category in dicCategories:
                    dicCategories[protocol.category] = 1
                else:
                    dicCategories[protocol.category] += 1


            data = []
            for category, count in dicCategories.items():
                data_row = ()
                data_row = (f'{datetime.datetime.now().date()}',
                            f'{category}',
                            count)

                data.append(data_row)
            # -----/

            columns = dicTableInfo['columns']
            connection.insert_to_table(table_name, columns, data)
            connection.add_to_action_log(table_name, self.action, len(data), '-')
            connection.close_connection()

        else:
            print(f'{table_name} already uploaded for {datetime.datetime.now().date()}')
            connection.close_connection()

    def pools_info(self):

        # DO NOT OVERRIDE ANY DATA
        # The ID is used as the Key to connect to pools_history
        # Only upload NEW pools that didn't exist before
        # Only pools that have at least 20% APY will be included (that will be also used for the pools_history)


        # ----- Get Table info
        table_name = 'pools_info'
        connection = Connection()
        dicTableInfo = connection.get_table_info(table_name)
        dicExistingPools = connection.get_uniq_values_from_col(table_name,'pool_id')
        # -----/

        if len(self.pools.list) == 0:
            self.pools.get_all_pools(getFreshData=True)

        data = []
        for pool in self.pools.list.values():
            if not pool.pool in dicExistingPools:
                if pool.apy > 19.9:

                    # ----- Reward tokens
                    if pool.rewardTokens is None or len(pool.rewardTokens) == 0:
                        reward_token_1 = None
                        reward_token_2 = None
                    elif len(pool.rewardTokens) == 1:
                        reward_token_1 = pool.rewardTokens[0]
                        reward_token_2 = None
                    else:
                        reward_token_1 = pool.rewardTokens[0]
                        reward_token_2 = pool.rewardTokens[1]
                    # ------/

                    # ------ Pool Underlying Tokens
                    pool_tokens = {}
                    pool_tokens[0] = None
                    pool_tokens[1] = None
                    pool_tokens[2] = None
                    if not pool.underlyingTokens is None:
                        for i in range(0,len(pool.underlyingTokens)):
                            pool_tokens[i] = pool.underlyingTokens[i]
                    # ------/


                    data_row = ()
                    data_row = (f'{pool.pool}',
                                f'{datetime.datetime.now().date()}',
                                f'{pool.chain[:35]}',
                                f'{pool.project[:40]}',
                                f'{pool.symbol[:100]}',
                                pool.stablecoin,
                                pool.ilRisk == 'yes',
                                pool.exposure == 'multi',
                                f'{pool.poolMeta[:255]}' if pool.poolMeta is not None else None,
                                f'{reward_token_1}' if reward_token_1 is not None else None,
                                f'{reward_token_2}' if reward_token_2 is not None else None,
                                f'{pool_tokens[0]}' if pool_tokens[0] is not None else None,
                                f'{pool_tokens[1]}' if pool_tokens[1] is not None else None,
                                f'{pool_tokens[2]}' if pool_tokens[2] is not None else None
                                )

                    data.append(data_row)
        # -----/

        if len(data) > 0:
            columns = dicTableInfo['columns']
            connection.insert_to_table(table_name, columns, data)
            connection.add_to_action_log(table_name, self.action, len(data), '-')
        else:
            connection.add_to_action_log(table_name, self.action, len(data), 'No new Pools')

        connection.close_connection()


    def token_contracts_gecko(self):

        # ----- Get Table info
        table_name = 'token_contracts'
        connection = Connection()
        dicTableInfo = connection.get_table_info(table_name)
        # -----/

        existing_contracts = connection.get_uniq_values_from_col(table_name, 'contract')

        gecko = Gecko()
        gecko.get_token_ids()

        data = []
        for token in gecko.tokens.values():
            for chain, contract in token.chains.items():
                if contract not in existing_contracts:

                    data_row = ()
                    data_row = (f'{datetime.datetime.now().date()}',
                                f'{token.symbol}',
                                f'{token.id}',
                                f'{chain.lower()}',
                                f'{contract}'
                                )

                    data.append(data_row)
        # -----/

        columns = dicTableInfo['columns']
        connection.insert_to_table(table_name, columns, data)
        connection.add_to_action_log(table_name, self.action, len(data), 'Update')
        connection.close_connection()



    def token_contracts(self):

        # ----- Get Table info
        table_name = 'token_contracts'
        connection = Connection()
        dicTableInfo = connection.get_table_info(table_name)
        # -----/

        existing_contracts = connection.get_uniq_values_from_col(table_name, 'contract')
        #connection.clear_whole_table(table_name)


        gecko = Gecko()
        gecko.get_token_ids()

        columns_clause = f"""token,
                            chain,
                            contract,
                            pool_count,
                            max_pc,
                            MIN(SUM(pool_count)) OVER(PARTITION BY contract) AS max2_pc,	-- Second largest
                            prospects,
                            index
                        """
        from_clause = f"""
                    (
                SELECT	token,
                        chain,
                        contract,
                        SUM(count) AS pool_count,
                        MAX(SUM(count)) OVER(PARTITION BY contract) AS max_pc,
                        COUNT(contract) OVER(PARTITION BY contract) AS prospects,
                        ROW_NUMBER() OVER(PARTITION BY contract ORDER BY SUM(count) DESC) AS index
                        
                  FROM	(
            
                SELECT	unnest(string_to_array(symbol, '-')) AS token,
                        chain,
                        LOWER(pool_token_1) as contract,
                        COUNT(id) AS COUNT
                  FROM 	pools_info
                 WHERE	pool_token_1 IS NOT NULL
                   AND	LENGTH(pool_token_1) > 15
                   AND	pool_token_3 IS NULL
              GROUP BY	unnest(string_to_array(symbol, '-')),
                        chain,
                        pool_token_1
             UNION ALL
                SELECT	unnest(string_to_array(symbol, '-')) AS token,
                        chain,
                        LOWER(pool_token_2) AS contract,
                        COUNT(id) AS COUNT
                  FROM 	pools_info
                 WHERE	pool_token_1 IS NOT NULL
                   AND	LENGTH(pool_token_2) > 15
                   AND	pool_token_3 IS NULL
              GROUP BY	unnest(string_to_array(symbol, '-')),
                        chain,
                        pool_token_2
                        
              ORDER BY	4 DESC
                    )
              GROUP BY	token,
                        chain,
                        contract
              ORDER BY	3 DESC
                      )
                    """
        where_clause = f"""
                    index < %s
                    """
        group_by = f"""
                    token,
                    chain,
                    contract,
                    pool_count,
                    max_pc,
                    index,
                    prospects
                    """
        order_by = f"""
                    pool_count DESC
                    """

        contracts_from_pools = connection.select_table_data(table_name=from_clause, columns=columns_clause
                                            ,where_clause=where_clause,group_by=group_by,order_by=order_by,params=(3,))
        df = pd.DataFrame(contracts_from_pools, columns=['token', 'chain','contract','pool_count','max_pc','max2_pc','prospects','index'])

        dicContracts = {}                       # Contract / tuple (token, chain, pool count)
        dicAssignedTokens_chains = {}           # Just to keep track of the assigned tokens and chains
        dicUnassigned = {}

        for index, row in df.iterrows():

            contract = row['contract']
            token = row['token']
            chain = row['chain']
            pool_count = row['pool_count']

            if contract not in existing_contracts:
                if contract not in dicContracts:

                    if contract.lower() in gecko.contracts:
                        gecko_contract = gecko.contracts[contract.lower()]
                        gecko_id = gecko_contract.id
                    else:
                        gecko_id = None


                    if row['max_pc'] != row['max2_pc'] and row['max_pc'] == row['pool_count']:

                        dicContracts[contract.lower()] = (token, chain, pool_count, gecko_id)
                        dicAssignedTokens_chains[token + '_' + chain] = contract

                    else:
                        if token + '_' + chain not in dicAssignedTokens_chains:
                            dicContracts[contract.lower()] = (token, chain, pool_count,gecko_id)
                            dicAssignedTokens_chains[token + '_' + chain] = contract
                        else:
                            if gecko_id is not None:
                                dicContracts[contract.lower()] = (gecko_contract.symbol, gecko_contract.chain, None, gecko_id)
                                dicAssignedTokens_chains[gecko_contract.symbol + '_' + gecko_contract.chain] = contract
                            else:

                                missingContract = 'E43qU77tnWDwN11o7TtaGMNpxCAqz8RZEZ7PcTCUXSim'
                                if contract.lower() == missingContract.lower():
                                    print(f'{token} -- {pool_count}')

                                if token not in ['WETH','WMATIC','USDC','USDT','SOL','WAVAX','GDAI','SDAI','WFTM','WBNB','BUSD']:
                                    #print(f'Unable to assign:  {contract} -- {token}')
                                    pass

        data = []
        if len(dicContracts) > 0:

            for contract, c_data in dicContracts.items():
                data_row = ()
                data_row = (f'{datetime.datetime.now().date()}',
                            f'{c_data[0]}',
                            f'{c_data[3]}' if c_data[3] is not None else None,
                            f'{c_data[1]}',
                            f'{contract}',
                            c_data[2]
                            )

                data.append(data_row)
        # -----/

       #     columns = dicTableInfo['columns']
       #     connection.insert_to_table(table_name, columns, data)
       # connection.add_to_action_log(table_name, self.action, len(data), 'Update')
        connection.close_connection()