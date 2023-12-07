import datetime
from dbTables import Tables_info
from dbManager import Connection
from datetime import date
from defiLlama import Pools, Protocols,Protocol, Chains



class Upload:

    def __init__(self):
        self.action = 'Data upload'
        self.tables_info = Tables_info()

        self.pools = Pools()
        self.protocols = Protocols()
        self.chains = Chains()


    def pools_history(self, overWriteTheDay: bool = False):

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


                data_row = ()
                data_row = (f'{pool.pool}',
                            f'{datetime.datetime.now().date()}',
                            f'{pool.chain[:35]}',
                            f'{pool.project[:40]}',
                            f'{pool.symbol[:100]}',
                            pool.tvlUsd,
                            pool.apyBase,
                            pool.apyReward,
                            pool.apy,
                            pool.stablecoin,
                            pool.ilRisk == 'yes',
                            pool.exposure == 'multi',
                            f'{pool.poolMeta[:255]}' if pool.poolMeta is not None else None,
                            pool.mu,
                            pool.sigma,
                            pool.count,
                            f'{reward_token_1}' if reward_token_1 is not None else None,
                            f'{reward_token_2}' if reward_token_2 is not None else None,
                            f'{pool_tokens[0]}' if pool_tokens[0] is not None else None,
                            f'{pool_tokens[1]}' if pool_tokens[1] is not None else None,
                            f'{pool_tokens[2]}' if pool_tokens[2] is not None else None,
                            pool.volumeUsd1d)

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
                            f'{protocol.name}',
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
                                f'{protocol.name}',
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


    def token_contracts(self):
        pass
