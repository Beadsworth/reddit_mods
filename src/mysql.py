import mysql_secret
import sqlalchemy as db
import pandas as pd


class MysqlClient:

    def __init__(self):

        self.engine = db.create_engine(mysql_secret.conn_str)

    def pull(self, query_str):

        result = pd.read_sql(sql=query_str, con=self.engine)
        return result

    def push(self, table_name, df):

        df.to_sql(name=table_name, con=self.engine, if_exists='append', index=False)

    def execute(self, query_str):

        with self.engine.connect() as con:
            con.execute(query_str)


if __name__ == '__main__':
    sql = MysqlClient()
    result = sql.pull('SELECT 1')
    print(result)
