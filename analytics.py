from dbManager import Connection
import pandas as pd

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
        where_clause = f"""apy > {min_apy}
                   AND	PH.date = CURRENT_DATE
                   AND	PH.tvl > {min_tvl}
                   AND	PI.symbol NOT LIKE '%UNKNOWN%'
                   AND LENGTH(PI.symbol) - LENGTH(REPLACE(PI.symbol, '-', '')) = 1"""
        order_by = f"""PH.apy DESC"""


        connection.insert_to_table_with_sql(table_name,to_columns=to_columns,from_table_name=from_table,columns=columns,
                                   where_clause=where_clause,order_by=order_by)


        dicInfo = connection.get_table_info(table_name)
        connection.add_to_action_log(table_name,'data_analytics',dicInfo['total_rows'],'Analytics Phase 1')
        connection.close_connection()

    def get_advanced_calcs(self):
        table_name = 'daily_pools_raw_selection'
        connection = Connection()

        pool_ids_dic = connection.get_uniq_values_from_col(table_name,'pool_info_id')
        pool_ids = pool_ids_dic.keys()

        dicInfo = connection.get_table_info('pools_history')
        columns_ph = dicInfo['columns']



        for id in pool_ids:

            pool_history = connection.select_table_data('pools_history',columns='*',where_clause=f'pool_info_id = {id}')
            df = pd.DataFrame(pool_history, columns=columns_ph)
            print(df)

            dicPoolData = {}
            dicPoolData['id'] = id                                          # Info table id
            dicPoolData['age'] = df.shape[0]                                # Age in days
            dicPoolData['apy'] = df['apy'].iloc[-1]                         # APY

            dicPoolData['apy_base'] = df['apy_base'].iloc[-1]               # APY Base
            dicPoolData['apy_reward'] = df['apy_reward'].iloc[-1]           # APY Reward

            dicPoolData['tvl'] = df['tvl'].iloc[-1]                         # TVL
            dicPoolData['volume'] = df['volume'].iloc[-1]                   # Volume
            dicPoolData['vol_to_tvl'] = df['vol_to_tvl'].iloc[-1]           # Volume to TVL



            if df.shape[0] > 2:
                dicPoolData['apy_change_3d'] = (dicPoolData['apy'] - df['apy'].iloc[-3]) / dicPoolData['apy']
            else:
                dicPoolData['apy_change_3d'] = None

            if df.shape[0] > 6:
                dicPoolData['apy_change_7d'] = (dicPoolData['apy'] - df['apy'].iloc[-7]) / dicPoolData['apy']
            else:
                dicPoolData['apy_change_7d'] = None

            if df.shape[0] > 13:
                dicPoolData['apy_change_14d'] = (dicPoolData['apy'] - df['apy'].iloc[-14]) / dicPoolData['apy']
            else:
                dicPoolData['apy_change_14d'] = None


        #pool = Pool(dicPoolData)            # Create Pool object

            #self.dicPools[id] = pool

        connection.close_connection()



class Pool:

    def __init__(self):
        self.db_info_id: float          # Info table ID

        self.age: int                   # Number of days
        self.apy: float                 # Total APY
        self.apy_change_3d: float       # APY change to the one 3 days ago
        self.apy_change_7d: float       # APY change to the one 7 days ago
        self.apy_change_14d: float      # APY change to the one 14 days ago
        self.apy_base: float            # Base APY
        self.apy_reward: float          # Reward APY

        self.tvl: float                 # Tvl (from database)
        self.volume: float              # Volume (from database)
        self.vol_to_tvl: float          # Volume / tvl (from database)

        self.chain: str                 # Chain name
        self.protocol: str              # Protocol name
        self.protocol_tvl: float        # TVL of the protocol the pool is in

        self.fee_rate: float            # Take from the meta (need to parse and doesn't exist for 80% of cases)

        self.tvl_change_3d: float       # Percentage change of tvl to the one 3 days ago
        self.tvl_change_7d: float       # Percentage change of tvl to the one 7 days ago
        self.tvl_change_14d: float      # Percentage change of tvl to the one 14 days ago

        self.asset_1: str               # Try to find out which one here is the actual token and which one is the collateral
        self.asset_2: str
        self.token_contract: str        #
        self.pair_contract: str

        self.live_price: float          # Most up to date price of the token
        self.live_tvl: float            # Most up to date tvl of the token
        self.live_volume: float         # Most up to date volume of the token
        self.live_vol_to_tvl: float     # Most up to date volume to tvl ratio

