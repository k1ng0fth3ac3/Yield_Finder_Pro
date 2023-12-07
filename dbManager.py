import datetime
import psycopg2
from keys import Postgres_keys


# Somehow ENSURE that the connection is always closed. Otherwise there can be data leakage and


class Connection:


    def __init__(self):

        self.conn = psycopg2.connect(host='localhost', dbname='defi_data', user=Postgres_keys.userName,
                            password=Postgres_keys.pw, port=5432)

        self.cur = self.conn.cursor()


    def close_connection(self):
        self.cur.close()
        self.conn.close()

    def create_table(self, table_name: str, dicCols: dict):
        col_def = ''
        for col, col_type in dicCols.items():
            if col_def != '':
                col_def += ',\n'
            col_def += f'{col} {col_type}'

        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {col_def}
        );
        """

        self.cur.execute(query)
        self.conn.commit()

        print(f'{table_name} created')

    def insert_to_table(self, table_name: str, columns: list, data: list):

        columns.remove('id')        # We don't need the ID col

        columns_str = ''
        values_str = ''
        for column in columns:
            if columns_str != '':
                columns_str = columns_str + ', '
                values_str = values_str + ', '
            columns_str = columns_str + column
            values_str = values_str + '%s'

        query = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({values_str});
        """

        self.cur.executemany(query, data)
        self.conn.commit()


    def update_records(self, table_name: str, columns: list, values: list, where_clause_sql: str):
        pass

    def delete_records(self, table_name, where_clause_sql: str):
        pass

    def truncate_table(self, table_name: str):
        pass

    def delete_table(self, table_name: str):
        pass

    def select_first_row(self, table_name: str):
        pass

    def select_last_row(self, table_name: str):
        pass

    def select_custom(self, sql_statement: str):
        pass

    def get_table_info(self, table_name: str):

        query = """
            SELECT * FROM action_log
            WHERE table_name = %s;
        """

        self.cur.execute(query, (table_name,))
        results = self.cur.fetchall()

        dicTableInfo = {
            'created_on' : None,                        # Table created on
            'total_log_entries': 0,                     # Total number of log entries
            'upload_count': 0,                          # Number of times we have uploaded data
            'upload_rows': 0,                           # Number of rows uploaded in total
            'upload_first_date': None,                  # First date of upload
            'upload_last_date': None                    # Last date of upload
        }

        for row in results:
            dicTableInfo['total_log_entries'] += 1

            if row[3] == 'Create table':
                dicTableInfo['created_on'] = row[1]

            if row[3] == 'Data upload':
                dicTableInfo['upload_count'] += 1
                dicTableInfo['upload_rows'] += row[5]

                if dicTableInfo['upload_first_date'] is None or dicTableInfo['upload_first_date'] > row[1]:
                    dicTableInfo['upload_first_date'] = row[1]

                if dicTableInfo['upload_last_date'] is None or dicTableInfo['upload_last_date'] < row[1]:
                    dicTableInfo['upload_last_date'] = row[1]


        query2 = f"""
        SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'
        """

        self.cur.execute(query2)
        dicTableInfo['columns'] = [row[0] for row in self.cur.fetchall()]


        return dicTableInfo

    def add_to_action_log(self, table_name: str, action: str, rows: float, note: str):
        now = datetime.datetime.now()
        date = now.date()
        time = now.time().replace(microsecond=0)

        query = f"""
        INSERT INTO action_log (date, time, action, table_name, rows, note)
        VALUES ('{date}', '{time}', '{action}', '{table_name}', {rows}, '{note}');
        """

        self.cur.execute(query)
        self.conn.commit()

        print(f'Added to log: {table_name} -- {action} -- {date} -- {rows}')

    def delete_log_entry(self, table_name: str, action: str, date = datetime.datetime.now().date()):
        query = f"""
        DELETE FROM action_log WHERE
        table_name = '{table_name}' AND action = '{action}' AND date = '{date}';
        """

        self.cur.execute(query)
        self.conn.commit()

        print(f'Deleted entry {table_name} -- {action} -- {date}')

    def delete_day_from_table(self, table_name: str, date = datetime.datetime.now().date()):
        query = f"""
        DELETE FROM {table_name} WHERE date = '{date}';
        """

        self.cur.execute(query)
        self.conn.commit()

        print(f'Deleted entries from table {table_name} for date {date}')

    def clear_whole_table(self, table_name: str):
        query = f"""
        DELETE FROM {table_name};
        """
        self.cur.execute(query)
        self.conn.commit()

        print(f'Cleared table {table_name}')