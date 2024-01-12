from dbManager import Connection
import pandas as pd
import re
from dexScreener import Contracts, Pair
from defiLlama import Price
import numpy as np


class Analytics:

    def __init__(self):
        self.dicPools = {}
        self.contracts = Contracts()


    def calc_everything(self, min_apy:int = 500, topNpools: int = 20):
        self.calc_daily_raw_pools(min_apy=min_apy)
        self.get_advanced_pool_data()
        self.rank_pools()
        self.get_pair_info(top_N_pools=topNpools)
        self.get_token_price_history(top_N_pools=topNpools)
        self.calc_price_analytics()
        self.calc_price_score()


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
                        CASE WHEN PH.volume IS NULL THEN 0 ELSE PH.volume END AS volume,
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
                   --AND  PH.volume IS NOT NULL
                   AND LENGTH(PI.symbol) - LENGTH(REPLACE(PI.symbol, '-', '')) = 1"""
        order_by = f"""PH.apy DESC"""


        connection.insert_to_table_with_sql(table_name,to_columns=to_columns,from_table_name=from_table,columns=columns,
                                   where_clause=where_clause,order_by=order_by,params=(min_apy, min_tvl, '%UNKNOWN%'))


        dicInfo = connection.get_table_info(table_name)
        #connection.add_to_action_log(table_name,'data_analytics',dicInfo['total_rows'],'Analytics Phase 1')
        print(f'{dicInfo["total_rows"]} rows added to table (testing - not added to action log')
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


            dayCounter = 0  #Reset
            for index in range(len(df) - 1, -1, -1):
                dayCounter +=1
                if dayCounter > 30:      # 30 days max
                    break

                dicPoolData['tvl_history'][len(df) - index -1] = df['tvl'].iloc[index]
                dicPoolData['apy_history'][len(df) - index-1] = df['apy'].iloc[index]


                if df['volume'].iloc[index] is not None:
                    dicPoolData['volume_history'][len(df) - index - 1] = df['volume'].iloc[index]
                    dicPoolData['vol_to_tvl_history'][len(df) - index-1] = df['volume'].iloc[index] / df['tvl'].iloc[index]

                    if df['volume'].iloc[index] / df['tvl'].iloc[index] < vol_to_tvl_min:
                        dicPoolData['vol_to_tvl_min_history'][len(df) - index - 1] = df['volume'].iloc[index] / df['tvl'].iloc[index]
                        vol_to_tvl_min = df['volume'].iloc[index] / df['tvl'].iloc[index]
                    else:
                        dicPoolData['vol_to_tvl_min_history'][len(df) - index - 1] = vol_to_tvl_min

                    if df['volume'].iloc[index] / df['tvl'].iloc[index] > vol_to_tvl_max:
                        dicPoolData['vol_to_tvl_max_history'][len(df) - index - 1] = df['volume'].iloc[index] / df['tvl'].iloc[index]
                        vol_to_tvl_max = df['volume'].iloc[index] / df['tvl'].iloc[index]
                    else:
                        dicPoolData['vol_to_tvl_max_history'][len(df) - index - 1] = vol_to_tvl_max

                    vol_to_tvl_sum = vol_to_tvl_sum + df['volume'].iloc[index] / df['tvl'].iloc[index]
                    dicPoolData['vol_to_tvl_avg_history'][len(df) - index - 1] = vol_to_tvl_sum / (len(df) - index)
                else:
                    dicPoolData['volume_history'][len(df) - index - 1] = 0
                    dicPoolData['vol_to_tvl_history'][len(df) - index - 1] = 0
                    dicPoolData['vol_to_tvl_min_history'][len(df) - index - 1] = 0
                    dicPoolData['vol_to_tvl_max_history'][len(df) - index - 1] = 0
                    dicPoolData['vol_to_tvl_avg_history'][len(df) - index - 1] = 0


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
                dicPoolData['base'] = dicPoolData['token_1']
                dicPoolData['quote'] = dicPoolData['token_2']
                dicPoolData['base_contract'] = dicPoolData['contract_1']
                dicPoolData['quote_contract'] = dicPoolData['contract_2']
                dicPoolData['gecko_id_base'] = dicPoolData['gecko_id_1']
                dicPoolData['gecko_id_quote'] = dicPoolData['gecko_id_2']
            else:
                dicPoolData['base'] = dicPoolData['token_2']
                dicPoolData['quote'] = dicPoolData['token_1']
                dicPoolData['base_contract'] = dicPoolData['contract_2']
                dicPoolData['quote_contract'] = dicPoolData['contract_1']
                dicPoolData['gecko_id_base'] = dicPoolData['gecko_id_2']
                dicPoolData['gecko_id_quote'] = dicPoolData['gecko_id_1']

            # We have too many results from DexScreener API if we get top pairs (like USDT/WBNB), so lets exclude those
            if not (token_1_index < 100 and token_2_index < 100):
                if not (dicPoolData['token_1'] is None or dicPoolData['token_2'] is None):
                    pool = Pool(dicPoolData)            # Create Pool object
                    self.dicPools[id] = pool            # Add to collection


        connection.close_connection()


    def rank_pools(self):

        dicScoreWeights = {}
        dicScoreWeights['daysAboveOne'] = 1
        dicScoreWeights['daysAbove0.5'] = 0.5
        dicScoreWeights['tvl_avg_3d'] = 0.5
        dicScoreWeights['tvl_avg_7d'] = 0.25
        dicScoreWeights['vol_to_tvl'] = 0.25
        dicScoreWeights['data_error_rate'] = -5

        dicScoreWeights['tvl_max'] = 1000000
        dicScoreWeights['tvl_max_points'] = 50
        dicScoreWeights['tvl_point_multiplier'] = 10


        for pool in self.dicPools.values():

            prevRate = 0        # Reset
            daysAboveOne = 0    # Reset
            dataErrors = 0      # Reset (number of days when the data was exaclty that of the previous day)
            for rate in pool.vol_to_tvl_history.values():

                if rate is not None:
                    if rate == prevRate:
                        dataErrors +=1

                    # Days above 1
                    if rate > 1:
                        daysAboveOne +=1

                    prevRate = rate


            if len(pool.vol_to_tvl_history) > 0:
                pool.vol_to_tvl_above_one_days = daysAboveOne
                pool.score = daysAboveOne * dicScoreWeights['daysAboveOne']

                pool.vol_to_tvl_above_one_rate = daysAboveOne / len(pool.vol_to_tvl_history)          #
                pool.score = pool.score + (daysAboveOne / len(pool.vol_to_tvl_history)) * 5


                if len(pool.vol_to_tvl_avg_history) > 2:
                    pool.vol_to_tvl_avg_3d = pool.vol_to_tvl_avg_history[2]
                    pool.score = pool.score + float(pool.vol_to_tvl_avg_history[2]) * dicScoreWeights['tvl_avg_3d']
                else:
                    pool.vol_to_tvl_avg_3d = None

                if len(pool.vol_to_tvl_avg_history) > 6:
                    pool.vol_to_tvl_avg_7d = pool.vol_to_tvl_avg_history[6]
                    pool.score = pool.score + float(pool.vol_to_tvl_avg_history[6]) * dicScoreWeights['tvl_avg_7d']
                else:
                    pool.vol_to_tvl_avg_7d = None

            else:
                pool.vol_to_tvl_above_one_days = None
                pool.vol_to_tvl_above_one_rate = None
                pool.vol_to_tvl_avg_3d = None
                pool.vol_to_tvl_avg_7d = None
                daysAboveOneRate = None
                pool.score = 0



            pool.score = pool.score + (dataErrors / pool.age) * dicScoreWeights['data_error_rate']

            if len(pool.vol_to_tvl_history) > 0:
                pool.score = pool.score + float(pool.vol_to_tvl_history[0]) * dicScoreWeights['vol_to_tvl']


                if float(pool.tvl_history[0]) >= dicScoreWeights['tvl_max']:
                    pool.score = pool.score + dicScoreWeights['tvl_max_points']
                else:
                    pool.score = pool.score + float(pool.tvl_history[0]) / 200000 * dicScoreWeights['tvl_point_multiplier']



        dicSorted_pools = sorted(self.dicPools.values(), key=lambda x: x.score, reverse=True)

        self.dicPools = {}  # Reset the dictionary

        for rank, pool in enumerate(dicSorted_pools,start=1):
            pool.rank = rank
            self.dicPools[pool.db_info_id] = pool


    def get_pair_info(self, top_N_pools: int = 10, max_tvl_delta: float = 0.4):

        dicPairs = {}       # base + %20 + quote

        for index, pool in enumerate(self.dicPools.values(), start=1):
            if index > top_N_pools:
                break

            if f'{pool.contract_base.lower()}%20{pool.quote_token.lower()}' not in dicPairs:
                dicPairs[f'{pool.contract_base.lower()}%20{pool.contract_quote.lower()}'] = 1


        #self.contracts.get_pairs(coin_address_list=contract_list, dicQuoteTokens=dicQuoteTokens)
        self.contracts.get_pairs_by_search(dicTokenPairs=dicPairs)

        for index, pool in enumerate(self.dicPools.values(), start=1):

            if index > top_N_pools:
                print(f'Broke on {pool.contract_base}')
                break

            if pool.contract_base in self.contracts.list:
                pairs = self.contracts.list[pool.contract_base]

                min_delta_tvl = 1
                min_delta_vol = 1

                # Check for the closest match (TVL the most reliable indicator)
                if pool.tvl_history is not None and 0 in pool.tvl_history:
                    for contract, pair in pairs.items():

                        if abs((float(pool.tvl_history[0]) - float(pair.liquidity_usd)) / float(pool.tvl_history[0])) < min_delta_tvl:
                            min_delta_tvl = abs((float(pool.tvl_history[0]) - float(pair.liquidity_usd)) / float(pool.tvl_history[0]))
                            closest_contract = contract     # Best one so far

                        if abs((float(pool.volume_history[0]) - float(pair.volumes['h24'])) / float(pool.volume_history[0])) < min_delta_vol:
                            min_delta_vol = abs((float(pool.volume_history[0]) - float(pair.volumes['h24'])) / float(pool.volume_history[0]))


                if min_delta_tvl != 1:
                    # High discrepency between DefiLlama TVL and the actual TVL (the APY might be hence incorrect)
                    pool.pair_contract = self.contracts.list[pool.contract_base][closest_contract]
                else:
                    # Couldn't find the pair at all (we have more than 30 matches and we retrieve only first 30 from API)
                    pool.pair_contract = None

                if min_delta_tvl < max_tvl_delta:
                    pool.data_discrepency = True  # Flag the pool
            else:
                pool.pair_contract = None


    def get_token_price_history(self,top_N_pools: int = 10):

        coin_id_list = []
        for pool in self.dicPools.values():

            if pool.gecko_id_base is not None:
                if pool.gecko_id_base not in coin_id_list:
                    coin_id_list.append(pool.gecko_id_base)


        price = Price()
        price.get_history_by_coinGecko(coin_id_list)

        tmp_dic = {}

        for pool in self.dicPools.values():
            if pool.gecko_id_base in price.history:
                pool.price_history = price.history[pool.gecko_id_base]
            else:
                pool.price_history = None


    def calc_price_analytics(self, dayRange_1: int = 14, dayRange_2:int = 7):

        # Trend (up vs down)
        # Trend strength (how much we moved)
        # Trend confidence (how often we moved with the trend)
        # Standard deviation
        # Volatility


        for pool in self.dicPools.values():
            pool.price_analytics = {}

            if pool.price_history is not None:

                price_list = list(pool.price_history.values())
                price_list.reverse()

                # ----------------------------------------
                if len(pool.price_history) >= dayRange_1:
                    pool.price_analytics[f'change_{dayRange_1}d'] = (pool.price_history[0] - \
                                            pool.price_history[dayRange_1-1]) / pool.price_history[dayRange_1 - 1]      # Change %
                    pool.price_analytics[f'stdev_{dayRange_1}d'] = np.std(price_list[:dayRange_1])                      # Stdev
                    pool.price_analytics[f'volatility_{dayRange_1}d'] = np.std(price_list[:dayRange_1]) \
                                                                        * np.sqrt(dayRange_1)                           # Volatility


                    positive_changes = sum(1 for i in range(len(price_list[:dayRange_1]) - 1) if price_list[i] > price_list[i + 1])
                    negative_changes = sum(1 for i in range(len(price_list[:dayRange_1]) - 1) if price_list[i] < price_list[i + 1])

                    if pool.price_analytics[f'change_{dayRange_1}d'] > 0.05:                                            # Trend
                        pool.price_analytics[f'trend_{dayRange_1}d'] = 'up'
                        pool.price_analytics[f'trend_confidence_{dayRange_1}d'] = positive_changes / dayRange_1         # Trend confidence
                    elif pool.price_analytics[f'change_{dayRange_1}d'] < -0.05:
                        pool.price_analytics[f'trend_{dayRange_1}d'] = 'down'
                        pool.price_analytics[f'trend_confidence_{dayRange_1}d'] = negative_changes / dayRange_1         # Trend confidence
                    else:
                        pool.price_analytics[f'trend_{dayRange_1}d'] = 'sideways'
                        pool.price_analytics[f'trend_confidence_{dayRange_1}d'] = (positive_changes - negative_changes)\
                                                                                  / dayRange_1                          # Trend confidence

                else:
                    pool.price_analytics[f'change_{dayRange_1}d'] = None
                    pool.price_analytics[f'stdev_{dayRange_1}d'] = None
                    pool.price_analytics[f'volatility_{dayRange_1}d'] = None
                    pool.price_analytics[f'trend_{dayRange_1}d'] = None
                    pool.price_analytics[f'trend_confidence_{dayRange_1}d'] = None

                if len(pool.price_history) >= dayRange_2:
                    pool.price_analytics[f'change_{dayRange_2}d'] = (pool.price_history[0] - \
                                            pool.price_history[dayRange_2-1]) / pool.price_history[dayRange_2-1]        # Change %
                    pool.price_analytics[f'stdev_{dayRange_2}d'] = np.std(price_list[:dayRange_2])                      # Stdev
                    pool.price_analytics[f'volatility_{dayRange_2}d'] = np.std(price_list[:dayRange_2]) \
                                                                        * np.sqrt(dayRange_2)                           # Volatility

                    positive_changes = sum(1 for i in range(len(price_list[:dayRange_2]) - 1) if price_list[i] > price_list[i + 1])
                    negative_changes = sum(1 for i in range(len(price_list[:dayRange_2]) - 1) if price_list[i] < price_list[i + 1])

                    if pool.price_analytics[f'change_{dayRange_2}d'] > 0.05:                                            # Trend
                        pool.price_analytics[f'trend_{dayRange_2}d'] = 'up'
                        pool.price_analytics[
                            f'trend_confidence_{dayRange_2}d'] = positive_changes / dayRange_2                          # Trend confidence
                    elif pool.price_analytics[f'change_{dayRange_2}d'] < -0.05:
                        pool.price_analytics[f'trend_{dayRange_2}d'] = 'down'
                        pool.price_analytics[
                            f'trend_confidence_{dayRange_2}d'] = negative_changes / dayRange_2                          # Trend confidence
                    else:
                        pool.price_analytics[f'trend_{dayRange_2}d'] = 'sideways'
                        pool.price_analytics[f'trend_confidence_{dayRange_2}d'] = (positive_changes - negative_changes) \
                                                                                  / dayRange_2                          # Trend confidence

                else:
                    pool.price_analytics[f'change_{dayRange_2}d'] = None
                    pool.price_analytics[f'stdev_{dayRange_2}d'] = None
                    pool.price_analytics[f'volatility_{dayRange_2}d'] = None
                    pool.price_analytics[f'trend_{dayRange_2}d'] = None
                    pool.price_analytics[f'trend_confidence_{dayRange_2}d'] = None
                # ----------------------------------------/


    def calc_price_score(self):
        # Positive price change is good, but if it's too much (100x), we get negative points
        #

        for pool in self.dicPools.values():
            if len(pool.price_analytics) > 0:
                # ---------- 14d Price change
                if 'change_14d' in pool.price_analytics and pool.price_analytics['change_14d'] is not None:
                    if pool.price_analytics['change_14d'] < 0:              # Negative trend
                        pool.price_score = pool.price_score + float(pool.price_analytics['change_14d']) * 10
                    elif pool.price_analytics['change_14d'] > 5:            # Too wild rise
                        pool.price_score = pool.price_score - 20
                    else:                                                   # Positive trend
                        pool.price_score = pool.price_score + float(pool.price_analytics['change_14d']) * 10
                # ----------/
                # ---------- 7d Price change
                if 'change_7d' in pool.price_analytics and pool.price_analytics['change_7d'] is not None:
                    if pool.price_analytics['change_7d'] < 0:              # Negative trend
                        pool.price_score = pool.price_score + float(pool.price_analytics['change_7d']) * 10
                    elif pool.price_analytics['change_7d'] > 5:            # Too wild rise
                        pool.price_score = pool.price_score - 20
                    else:                                                   # Positive trend
                        pool.price_score = pool.price_score + float(pool.price_analytics['change_7d']) * 10
                # ----------/

                # ----------Standard deviation
                if 'stdev_14d' in pool.price_analytics and pool.price_analytics['stdev_14d'] is not None:
                    pool.price_score = pool.price_score + (1-float(pool.price_analytics['stdev_14d'])) * 10
                if 'stdev_7d' in pool.price_analytics and pool.price_analytics['stdev_7d'] is not None:
                    pool.price_score = pool.price_score + (1-float(pool.price_analytics['stdev_7d'])) * 10
                # ----------/
                # ---------- Volatility
                if 'volatility_14d' in pool.price_analytics and pool.price_analytics['volatility_14d'] is not None:
                    pool.price_score = pool.price_score + float(pool.price_analytics['volatility_14d']) * -7.5
                if 'volatility_7d' in pool.price_analytics and pool.price_analytics['volatility_7d'] is not None:
                    pool.price_score = pool.price_score + float(pool.price_analytics['volatility_7d']) * -7.5
                # ----------/

                if pool.price_score < -100:
                    pool.price_score = -100

                pool.total_score = pool.score + pool.price_score
            else:
                pool.total_score = 0


        dicSorted_pools = sorted(self.dicPools.values(), key=lambda x: x.price_score, reverse=True)
        self.dicPools = {}  # Reset the dictionary

        rank = 0    # Reset
        for pool in dicSorted_pools:
            #if pool.total_score > 0:
            rank +=1
            pool.rank = rank
            self.dicPools[pool.db_info_id] = pool


    def final_Scoring(self):
        # Final scode = Risk score + tvl_vol score + price score + apy score
        # Risk score = FDV / 100000000 + TVL / 100000
        # APY score = APY * 0.15 for scores below 100, 100 + APY * 0.025 for scores above 100, max 300

        for pool in self.dicPools.values():

            if float(pool.apy_base) * 0.15 > 100:
                if 100 + float(pool.apy_base) * 0.025 > 300:
                    pool.apy_score = 300
                else:
                    pool.apy_score = 100 + float(pool.apy_base) * 0.025
            else:
                pool.apy_score = float(pool.apy_base) * 0.15


            if pool.tvl_history is not None and pool.pair_contract is not None:
                pool.risk_score = float(pool.pair_contract.fdv) / 100000000 + float(pool.tvl_history[0]) / 100000
            else:
                pool.risk_score = 0


            pool.final_score = pool.risk_score + pool.apy_score + pool.score + pool.price_score


        dicSorted_pools = sorted(self.dicPools.values(), key=lambda x: x.final_score, reverse=True)
        self.dicPools = {}  # Reset the dictionary

        rank = 0    # Reset
        for pool in dicSorted_pools:
            if pool.final_score > 0:
                rank +=1
                pool.rank = rank
                self.dicPools[pool.db_info_id] = pool





class Pool:

    def __init__(self, dicPoolData):
        self.db_info_id: float                      # Info table ID
        self.symbol: str                            # Pair symbol

        self.age: int                               # Number of days
        self.apy_base: float                        # Base APY
        self.apy_reward: float                      # Reward APY

        self.chain: str                             # Chain name
        self.protocol: str                          # Protocol name

        self.fee_rate: float                        # Take from the meta (need to parse and doesn't exist for 80% of cases)

        self.data_discrepency: bool = False          # Discrepency between DefiLlama and
        self.tvl_history: dict                      # TVl history (0 = today, 1 = yesterday...)
        self.volume_history: dict                   # Volume history (0 = today, 1 = yesterday...)
        self.apy_history: dict                      # APY history (0 = today, 1 = yesterday...)
        self.vol_to_tvl_history: dict               # Volume to TVL history (0 = today, 1 = yesterday...)

        self.vol_to_tvl_min_history: dict           # Minimum vol to tvl rate for each day (0 = today, 1 = yesterday...)
        self.vol_to_tvl_max_history: dict           # Maximum vol to tvl rate for each day (0 = today, 1 = yesterday...)
        self.vol_to_tvl_avg_history: dict           # AVG vol to tvl rate for each day (0 = today, 1 = yesterday...)

        self.base_token: str                        # Primary focus token
        self.quote_token: str                       # Pair token (ETH, AVAX, SOL, etc.)
        self.contract_base: str                     # Contract address 1
        self.contract_quote: str                    # Contract address 2
        self.gecko_id_base: str                     # Gecko id 1
        self.gecko_id_quote: str                    # Gecko id 2

        self.vol_to_tvl_above_one_days: float       # Days above 1
        self.vol_to_tvl_above_one_rate: float       # Rate of days above 1 versus total days
        self.vol_to_tvl_avg_3d: float               # Average rate from past 3 days
        self.vol_to_tvl_avg_7d: float               # Average rate from past 7 days


        self.score: float                           # Total score for the pool
        self.rank: int                              # Rank based on the score
        self.price_score: float = 0                 # Score based on the price
        self.total_score: float = 0                 # Previous score + price score
        self.risk_score: float = 0                  # Higher the better (based on TVL and FDV)
        self.apy_score: float = 0
        self.final_score: float = 0                 # tvl_to_vol score + price score + risk score

        self.pair_contract: Pair = None             # Pair object (retrieved from DexTools API)
        self.price_history: dict                    # Key: distance in days from today, value: Price

        self.price_analytics: dict                  # trend_Nd, trend_confidence_Nd, change_Nd, volatility_Nd, stdev, volatility

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

        self.base_token = dicPoolData['base']
        self.quote_token = dicPoolData['quote']
        self.contract_base = dicPoolData['base_contract'].lower()
        self.contract_quote = dicPoolData['quote_contract'].lower()
        self.gecko_id_base = dicPoolData['gecko_id_base']
        self.gecko_id_quote = dicPoolData['gecko_id_quote']

        self.tvl_history = dicPoolData['tvl_history']
        self.apy_history = dicPoolData['apy_history']
        self.volume_history = dicPoolData['volume_history']
        self.vol_to_tvl_history = dicPoolData['vol_to_tvl_history']

        self.vol_to_tvl_min_history = dicPoolData['vol_to_tvl_min_history']
        self.vol_to_tvl_max_history = dicPoolData['vol_to_tvl_max_history']
        self.vol_to_tvl_avg_history = dicPoolData['vol_to_tvl_avg_history']