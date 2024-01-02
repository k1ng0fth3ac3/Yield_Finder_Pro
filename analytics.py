from dbManager import Connection
import pandas as pd
import re

class Analytics:

    def __init__(self):
        self.dicPools = {}


    def calc_daily_raw_pools(self, min_apy: int = 500, min_tvl: float = 20000):


        table_name = 'daily_pools_raw_selection'
        connection = Connection()
        connection.clear_whole_table(table_name)

        to_columns = 'symbol, pool_info_id, project, chain, apy, tvl, volume, vol_tvl_rate, apy_base, apy_reward, protocol_tvl, pool_meta'
        from_table = f"""pools_history AS PH
                  LEFT	OUTER JOIN pools_info AS PI
                    ON	PH.pool_info_id = PI.id
                  LEFT	OUTER JOIN protocols_info PRI
                    ON	PRI.slug = PI.project
                  LEFT	OUTER JOIN protocols_history PRH
                    ON	PRH.protocol_id = PRI.protocol_id AND PRH.date = CURRENT_DATE"""
        columns = f"""PI.symbol,
                        PI.id as pool_info_id,
                        PI.project,
                        PI.chain,
                        ROUND(PH.apy,1) AS apy,
                        PH.tvl,
                        PH.volume,
                        ROUND(CASE WHEN PH.volume IS NOT NULL THEN PH.volume / PH.tvl ELSE NULL END, 2) AS vol_tvl_rate,
                        ROUND(PH.apy_base,1) AS apy_base,
                        ROUND(PH.apy_reward,1) AS apy_reward,
                        PRH.tvl AS protocol_tvl,
                        PI.pool_meta"""
        where_clause = f"""apy_base > %s
                   AND	PH.date = CURRENT_DATE
                   AND	PH.tvl > %s
                   AND	PI.symbol NOT LIKE %s
                   AND  PI.pool_token_1 IS NOT NULL
                   AND  PH.volume IS NOT NULL
                   AND LENGTH(PI.symbol) - LENGTH(REPLACE(PI.symbol, '-', '')) = 1"""
        order_by = f"""PH.apy DESC"""


        connection.insert_to_table_with_sql(table_name,to_columns=to_columns,from_table_name=from_table,columns=columns,
                                   where_clause=where_clause,order_by=order_by,params=(min_apy, min_tvl, '%UNKNOWN%'))


        dicInfo = connection.get_table_info(table_name)
        connection.add_to_action_log(table_name,'data_analytics',dicInfo['total_rows'],'Analytics Phase 1')
        #print(f'{dicInfo["total_rows"]} rows added to table (testing - not added to action log')
        connection.close_connection()

    def get_advanced_pool_data(self):
        table_name = 'daily_pools_raw_selection'
        connection = Connection()

        pool_ids_dic = connection.get_uniq_values_from_col(table_name,'pool_info_id')
        pool_ids = pool_ids_dic.keys()

        dicInfo = connection.get_table_info('pools_history')
        columns_ph = dicInfo['columns']
        dicInfo = connection.get_table_info('pools_info')
        columns_pi = dicInfo['columns']
        dicInfo = connection.get_table_info('token_contracts')
        columns_tc = dicInfo['columns']

        percentage_pattern = r'(\d+(\.\d+)?)%'  # Regular expression for matching percentage values

        pd.set_option('display.max_columns', 15)

        for id in pool_ids:

            pool_history = connection.select_table_data('pools_history',columns='*',where_clause=f'pool_info_id = {id}')
            df = pd.DataFrame(pool_history, columns=columns_ph)

            dicPoolData = {}
            dicPoolData['id'] = id                                                          # Info table id
            dicPoolData['age'] = df.shape[0]                                                # Age in days

            #dicPoolData['apy'] = df['apy'].iloc[-1]                                         # APY
            dicPoolData['apy_base'] = df['apy_base'].iloc[-1]                               # APY Base
            dicPoolData['apy_reward'] = df['apy_reward'].iloc[-1]                           # APY Reward

            #dicPoolData['volume'] = df['volume'].iloc[-1]                                   # Volume
            #dicPoolData['vol_to_tvl'] = dicPoolData['volume'] / dicPoolData['tvl']          # Volume to TVL

            dicPoolData['tvl_history'] = {}
            dicPoolData['apy_history'] = {}
            dicPoolData['volume_history'] = {}
            dicPoolData['vol_to_tvl_history'] = {}
            dicPoolData['vol_to_tvl_min_history'] = {}
            dicPoolData['vol_to_tvl_max_history'] = {}
            dicPoolData['vol_to_tvl_avg_history'] = {}


            vol_to_tvl_min = 9999       # Reset
            vol_to_tvl_max = 0          # Reset
            vol_to_tvl_sum = 0          # Reset


            for index in range(len(df) - 1, -1, -1):
                if index > 30:
                    break

                dicPoolData['tvl_history'][len(df) - index -1] = df['tvl'].iloc[index]
                dicPoolData['volume_history'][len(df) - index -1] = df['volume'].iloc[index]
                dicPoolData['apy_history'][len(df) - index-1] = df['apy'].iloc[index]
                dicPoolData['vol_to_tvl_history'][len(df) - index-1] = df['volume'].iloc[index] / df['tvl'].iloc[index]

                if df['volume'].iloc[index] / df['tvl'].iloc[index] < vol_to_tvl_min:
                    dicPoolData['vol_to_tvl_min_history'][len(df) - index - 1] = df['volume'].iloc[index] / df['tvl'].iloc[index]
                    vol_to_tvl_min = df['volume'].iloc[index] / df['tvl'].iloc[index]

                if df['volume'].iloc[index] / df['tvl'].iloc[index] > vol_to_tvl_max:
                    dicPoolData['vol_to_tvl_max_history'][len(df) - index - 1] = df['volume'].iloc[index] / df['tvl'].iloc[index]
                    vol_to_tvl_max = df['volume'].iloc[index] / df['tvl'].iloc[index]

                vol_to_tvl_sum = vol_to_tvl_sum + df['volume'].iloc[index] / df['tvl'].iloc[index]
                dicPoolData['vol_to_tvl_avg_history'][len(df) - index - 1] = vol_to_tvl_sum / (len(df) - index)


            pool_info = connection.select_table_data('pools_info', columns='*', where_clause='id = %s', params=(id,))
            df = pd.DataFrame(pool_info, columns=columns_pi)

            dicPoolData['symbol'] = df['symbol'].iloc[0]
            dicPoolData['chain'] = df['chain'].iloc[0]
            dicPoolData['protocol'] = df['project'].iloc[0]

            dicPoolData['contract_1'] = df['pool_token_1'].iloc[0].lower()
            dicPoolData['contract_2'] = df['pool_token_2'].iloc[0].lower()


            if df['pool_meta'].iloc[0] is not None:
                percent_match = re.search(percentage_pattern, df['pool_meta'].iloc[0])
                if percent_match:
                    dicPoolData['fee_rate'] = float(percent_match.group(1))
                else:
                    dicPoolData['fee_rate'] = None
            else:
                dicPoolData['fee_rate'] = None

            token_1 = connection.select_table_data('token_contracts', columns='*', where_clause=f'contract = %s',
                                                   params=(dicPoolData['contract_1'],))

            df = pd.DataFrame(token_1, columns=columns_tc)
            if df.shape[0] > 0:
                dicPoolData['token_1'] = df['token'].iloc[0]
                dicPoolData['gecko_id_1'] = df['gecko_id'].iloc[0]
                token_1_index = df['id'].iloc[0]        # Check this index against the token_2 index. Lower = Primary token
            else:
                dicPoolData['token_1'] = None
                dicPoolData['gecko_id_1'] = None


            token_2 = connection.select_table_data('token_contracts', columns='*', where_clause=f'contract = %s',
                                                   params=(dicPoolData['contract_2'],))
            df = pd.DataFrame(token_2, columns=columns_tc)
            if df.shape[0] > 0:
                dicPoolData['token_2'] = df['token'].iloc[0]
                dicPoolData['gecko_id_2'] = df['gecko_id'].iloc[0]
                token_2_index = df['id'].iloc[0]  # Check this index against the token_1 index. Lower = Primary token
            else:
                dicPoolData['token_2'] = None
                dicPoolData['gecko_id_2'] = None


            # Get the Primary vs base token
            # The Base tokens (ETH, AVAX, SOL, etc) are high up as a token index in token_contracts table
            if token_1_index > token_2_index:
                dicPoolData['primary'] = dicPoolData['token_1']
                dicPoolData['base'] = dicPoolData['token_2']
                dicPoolData['primary_contract'] = dicPoolData['contract_1']
                dicPoolData['base_contract'] = dicPoolData['contract_2']
                dicPoolData['gecko_id_primary'] = dicPoolData['gecko_id_1']
                dicPoolData['gecko_id_base'] = dicPoolData['gecko_id_2']
            else:
                dicPoolData['primary'] = dicPoolData['token_2']
                dicPoolData['base'] = dicPoolData['token_1']
                dicPoolData['primary_contract'] = dicPoolData['contract_2']
                dicPoolData['base_contract'] = dicPoolData['contract_1']
                dicPoolData['gecko_id_primary'] = dicPoolData['gecko_id_2']
                dicPoolData['gecko_id_base'] = dicPoolData['gecko_id_1']


            pool = Pool(dicPoolData)            # Create Pool object
            self.dicPools[id] = pool            # Add to collection


        connection.close_connection()


    def filter_pools(self):

        for pool in self.dicPools.values():

            # AGE
            if pool.age < 2:
                age_score = 0
            else:
                age_score = pool.age / 5


            # Vol to tvl history
            # Get min and Max



class Pool:

    def __init__(self, dicPoolData):
        self.db_info_id: float          # Info table ID
        self.symbol: str                # Pair symbol

        self.age: int                   # Number of days
        self.apy_base: float            # Base APY
        self.apy_reward: float          # Reward APY

        self.chain: str                 # Chain name
        self.protocol: str              # Protocol name

        self.fee_rate: float            # Take from the meta (need to parse and doesn't exist for 80% of cases)

        self.tvl_history: dict          # TVl history (0 = today, 1 = yesterday...)
        self.volume_history: dict       # Volume history (0 = today, 1 = yesterday...)
        self.apy_history: dict          # APY history (0 = today, 1 = yesterday...)
        self.vol_to_tvl_history: dict   # Volume to TVL history (0 = today, 1 = yesterday...)

        self.vol_to_tvl_min_history: dict   # Minimum vol to tvl rate for each day (0 = today, 1 = yesterday...)
        self.vol_to_tvl_max_history: dict   # Maximum vol to tvl rate for each day (0 = today, 1 = yesterday...)
        self.vol_to_tvl_avg_history: dict   # AVG vol to tvl rate for each day (0 = today, 1 = yesterday...)

        self.primary_token: str         # Primary focus token
        self.base_token: str            # Pair token (ETH, AVAX, SOL, etc.)
        self.contract_primary: str      # Contract address 1
        self.contract_base: str         # Contract address 2
        self.gecko_id_primary: str      # Gecko id 1
        self.gecko_id_base: str         # Gecko id 2

        self.live_price: float          # Most up to date price of the token
        self.live_tvl: float            # Most up to date tvl of the token
        self.live_volume: float         # Most up to date volume of the token
        self.live_vol_to_tvl: float     # Most up to date volume to tvl ratio


        self.parse(dicPoolData)

    def parse(self,dicPoolData):
        self.db_info_id = dicPoolData['id']
        self.symbol = dicPoolData['symbol']
        self.age = dicPoolData['age']

        self.apy_base = dicPoolData['apy_base']
        self.apy_reward = dicPoolData['apy_reward']

        self.chain = dicPoolData['chain']
        self.protocol = dicPoolData['protocol']
        self.fee_rate = dicPoolData['fee_rate']

        self.primary_token = dicPoolData['primary']
        self.base_token = dicPoolData['base']
        self.contract_primary = dicPoolData['primary_contract']
        self.contract_base = dicPoolData['base_contract']
        self.gecko_id_primary = dicPoolData['gecko_id_primary']
        self.gecko_id_base = dicPoolData['gecko_id_base']

        self.tvl_history = dicPoolData['tvl_history']
        self.apy_history = dicPoolData['apy_history']
        self.volume_history = dicPoolData['volume_history']
        self.vol_to_tvl_history = dicPoolData['vol_to_tvl_history']

        self.vol_to_tvl_min_history = dicPoolData['vol_to_tvl_min_history']
        self.vol_to_tvl_max_history = dicPoolData['vol_to_tvl_max_history']
        self.vol_to_tvl_avg_history = dicPoolData['vol_to_tvl_avg_history']