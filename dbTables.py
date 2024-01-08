from dbManager import Connection



class Create_table:

    def __init__(self, connObj: Connection):
        self.connection = connObj
        self.action = 'Create table'
        self.table_info = Tables_info()

    def action_log(self):
        table_name = 'action_log'

        dicCols = self.table_info.action_log()

        self.connection.create_table(table_name,dicCols)
        self.connection.add_to_action_log(table_name,self.action,0,f'{len(dicCols)} columns')

    def chains_info(self):
        table_name = 'chains_info'

        dicCols = self.table_info.chains_info()

        self.connection.create_table(table_name,dicCols)
        self.connection.add_to_action_log(table_name,self.action,0,f'{len(dicCols)} columns')

    def chains_history(self):
        table_name = 'chains_history'

        dicCols = self.table_info.chains_history()

        self.connection.create_table(table_name,dicCols)
        self.connection.add_to_action_log(table_name,self.action,0,f'{len(dicCols)} columns')

    def chains_history(self):
        table_name = 'chains_history'

        dicCols = self.table_info.chains_history()

        self.connection.create_table(table_name,dicCols)
        self.connection.add_to_action_log(table_name,self.action,0,f'{len(dicCols)} columns')

    def pools_history(self):
        table_name = 'pools_history'

        dicCols = self.table_info.pools_history()

        self.connection.create_table(table_name,dicCols)
        self.connection.add_to_action_log(table_name,self.action,0,f'{len(dicCols)} columns')


    def protocols_info(self):
        table_name = 'protocols_info'

        dicCols = self.table_info.protocols_info()
        self.connection.create_table(table_name,dicCols)
        self.connection.add_to_action_log(table_name,self.action,0,f'{len(dicCols)} columns')

    def protocols_history(self):
        table_name = 'protocols_history'

        dicCols = self.table_info.protocols_history()

        self.connection.create_table(table_name,dicCols)
        self.connection.add_to_action_log(table_name,self.action,0,f'{len(dicCols)} columns')


    def protocols_chains_history(self):
        table_name = 'protocols_chains_history'

        dicCols = self.table_info.protocols_chains_history()

        self.connection.create_table(table_name,dicCols)
        self.connection.add_to_action_log(table_name,self.action,0,f'{len(dicCols)} columns')

    def categories_history(self):
        table_name = 'categories_history'

        dicCols = self.table_info.categories_history()

        self.connection.create_table(table_name, dicCols)
        self.connection.add_to_action_log(table_name, self.action, 0, f'{len(dicCols)} columns')

    def pools_info(self):
        table_name = 'pools_info'

        dicCols = self.table_info.pools_info()

        self.connection.create_table(table_name, dicCols)
        self.connection.add_to_action_log(table_name, self.action, 0, f'{len(dicCols)} columns')

    def daily_pools_raw_selection(self):
        table_name = 'daily_pools_raw_selection'

        dicCols = self.table_info.daily_pools_raw_selection()

        self.connection.create_table(table_name, dicCols)
        self.connection.add_to_action_log(table_name, self.action, 0, f'{len(dicCols)} columns')

    def token_contracts(self):
        table_name = 'token_contracts'

        dicCols = self.table_info.token_contracts()

        self.connection.create_table(table_name, dicCols)
        self.connection.add_to_action_log(table_name, self.action, 0, f'{len(dicCols)} columns')

    def result_table(self):
        table_name = 'result_table'

        dicCols = self.table_info.result_table()

        self.connection.create_table(table_name, dicCols)
        self.connection.add_to_action_log(table_name, self.action, 0, f'{len(dicCols)} columns')


class Tables_info:


    def action_log(self):
        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['date'] = 'DATE'
        dicCols['time'] = 'TIME'
        dicCols['action'] = 'VARCHAR(20)'
        dicCols['table_name'] = 'VARCHAR(50)'
        dicCols['rows'] = 'DECIMAL(8,0)'
        dicCols['note'] = 'VARCHAR(255)'

        return dicCols

    def chains_info(self):

        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['chain'] = 'VARCHAR(40)'
        dicCols['chain_id'] = 'INT'
        dicCols['gecko_id'] = 'VARCHAR(100)'
        dicCols['cmc_id'] = 'INT'
        dicCols['token_symbol'] = 'VARCHAR(20)'

        return dicCols

    def chains_history(self):

        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['chain'] = 'VARCHAR(40)'
        dicCols['date'] = 'DATE'
        dicCols['tvl'] = 'DECIMAL(12,0)'
        dicCols['volume'] = 'DECIMAL(12,0)'
        dicCols['fees'] = 'DECIMAL(12,0)'
        dicCols['protocols'] = 'INT'
        dicCols['pools'] = 'INT'
        dicCols['stables_circulating'] = 'DECIMAL(14,0)'

        return dicCols

    def pools_history(self):

        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['pool_id'] = 'VARCHAR(40)'
        dicCols['date'] = 'DATE'
        dicCols['tvl'] = 'DECIMAL(12,0)'
        dicCols['apy_base'] = 'DECIMAL(12,4)'
        dicCols['apy_reward'] = 'DECIMAL(12,4)'
        dicCols['apy'] = 'DECIMAL(12,4)'
        dicCols['volume'] = 'DECIMAL(12,0)'
        dicCols['pool_info_id'] = 'INT'

        return dicCols

    def protocols_info(self):

        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['protocol_id'] = 'INT'
        dicCols['name'] = 'VARCHAR(100)'
        dicCols['chain'] = 'VARCHAR(40)'
        dicCols['symbol'] = 'VARCHAR(20)'
        dicCols['url'] = 'VARCHAR(255)'
        dicCols['listed_at'] = 'DECIMAL(10,0)'
        dicCols['gecko_id'] = 'VARCHAR(50)'
        dicCols['twitter'] = 'VARCHAR(100)'
        dicCols['category'] = 'VARCHAR(50)'
        dicCols['slug'] = 'VARCHAR(40)'

        return dicCols


    def protocols_history(self):

        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['protocol_id'] = 'INT'
        dicCols['date'] = 'DATE'
        dicCols['tvl'] = 'DECIMAL(12,0)'
        dicCols['volume'] = 'DECIMAL(12,0)'
        dicCols['fees'] = 'DECIMAL(10,0)'
        dicCols['revenue'] = 'DECIMAL(10,0)'
        dicCols['user_fees'] = 'DECIMAL(10,0)'
        dicCols['holders_revenue'] = 'DECIMAL(10,0)'
        dicCols['creator_revenue'] = 'DECIMAL(10,0)'
        dicCols['supply_side_revenue'] = 'DECIMAL(10,0)'
        dicCols['protocol_revenue'] = 'DECIMAL(10,0)'


        return dicCols

    def protocols_chains_history(self):

        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['chain'] = 'VARCHAR(35)'
        dicCols['date'] = 'DATE'
        dicCols['protocol_id'] = 'INT'
        dicCols['tvl'] = 'DECIMAL(12,0)'
        dicCols['volume'] = 'DECIMAL(12,0)'
        dicCols['fees'] = 'DECIMAL(12,0)'

        return dicCols

    def categories_history(self):
        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['date'] = 'DATE'
        dicCols['category'] = 'VARCHAR(50)'
        dicCols['protocol_count'] = 'INT'

        return dicCols

    def pools_info(self):
        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['pool_id'] = 'VARCHAR(40)'
        dicCols['date_added'] = 'DATE'
        dicCols['chain'] = 'VARCHAR(35)'
        dicCols['project'] = 'VARCHAR(40)'
        dicCols['symbol'] = 'VARCHAR(100)'
        dicCols['stable_coin'] = 'BOOL'
        dicCols['il_risk'] = 'BOOL'
        dicCols['exposure_multi'] = 'BOOL'
        dicCols['pool_meta'] = 'VARCHAR(255)'
        dicCols['reward_token_1'] = 'VARCHAR(255)'
        dicCols['reward_token_2'] = 'VARCHAR(255)'
        dicCols['pool_token_1'] = 'VARCHAR(255)'
        dicCols['pool_token_2'] = 'VARCHAR(255)'
        dicCols['pool_token_3'] = 'VARCHAR(255)'

        return dicCols


    def daily_pools_raw_selection(self):
        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['symbol'] = 'VARCHAR(100)'
        dicCols['pool_info_id'] = 'INT'
        dicCols['project'] = 'VARCHAR(40)'
        dicCols['chain'] = 'VARCHAR(35)'

        dicCols['apy'] = 'DECIMAL(12,1)'
        dicCols['tvl'] = 'DECIMAL(12,0)'
        dicCols['volume'] = 'DECIMAL(12,0)'
        dicCols['vol_tvl_rate'] = 'DECIMAL(12,3)'
        dicCols['apy_base'] = 'DECIMAL(12,1)'
        dicCols['apy_reward'] = 'DECIMAL(12,1)'
        dicCols['protocol_tvl'] = 'DECIMAL(16,0)'
        dicCols['pool_meta'] = 'VARCHAR(255)'

        return dicCols

    def token_contracts(self):
        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['date_added'] = 'DATE'
        dicCols['token'] = 'VARCHAR(100)'
        dicCols['gecko_id'] = 'VARCHAR(100)'
        dicCols['chain'] = 'VARCHAR(35)'
        dicCols['contract'] = 'VARCHAR(255)'
        dicCols['pool_count'] = 'INT'

        return dicCols


    def result_table(self):
        dicCols = {}
        dicCols['id'] = 'SERIAL PRIMARY KEY'
        dicCols['date_added'] = 'DATE'
        dicCols['rank'] = 'INT'
        dicCols['total_score'] = 'DECIMAL(5,2)'
        dicCols['tvl_vol_score'] = 'DECIMAL(5,2)'
        dicCols['price_score'] = 'DECIMAL(5,2)'

        dicCols['symbol'] = 'VARCHAR(20)'
        dicCols['age'] = 'INT'
        dicCols['apy_base'] = 'DECIMAL(12,4)'
        dicCols['chain'] = 'VARCHAR(35)'
        dicCols['protocol'] = 'VARCHAR(100)'
        dicCols['fee_rate'] = 'DECIMAL(5,4)'

        dicCols['pair_address'] = 'VARCHAR(255)'
        dicCols['base_token'] = 'VARCHAR(100)'
        dicCols['quote_token'] = 'VARCHAR(100)'
        dicCols['contract_base'] = 'VARCHAR(255)'
        dicCols['contract_quote'] = 'VARCHAR(255)'
        dicCols['gecko_id_base'] = 'VARCHAR(100)'
        dicCols['gecko_id_quote'] = 'VARCHAR(100)'

        dicCols['fdv'] = 'DECIMAL(14,0)'
        dicCols['price'] = 'DECIMAL(16,10)'
        dicCols['tvl'] = 'DECIMAL(14,0)'
        dicCols['volume'] = 'DECIMAL(14,0)'
        dicCols['vol_tvl'] = 'DECIMAL(6,3)'

        dicCols['vol_tvl_above1_rate'] = 'DECIMAL(5,3)'
        dicCols['vol_to_tvl_avg_3d'] = 'DECIMAL(5,3)'
        dicCols['vol_to_tvl_avg_7d'] = 'DECIMAL(5,3)'

        dicCols['price_trend_14d'] = 'VARCHAR(10)'
        dicCols['price_trend_7d'] = 'VARCHAR(10)'
        dicCols['price_trend_confidence_14d'] = 'DECIMAL(5,3)'
        dicCols['price_trend_confidence_7d'] = 'DECIMAL(5,3)'
        dicCols['price_change_14d'] = 'DECIMAL(5,3)'
        dicCols['price_change_7d'] = 'DECIMAL(5,3)'
        dicCols['price_stdev_14d'] = 'DECIMAL(5,4)'
        dicCols['price_stdev_7d'] = 'DECIMAL(5,4)'
        dicCols['price_volatility_14d'] = 'DECIMAL(6,4)'
        dicCols['price_volatility_7d'] = 'DECIMAL(6,4)'

        return dicCols