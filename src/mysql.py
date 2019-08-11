import mysql_secret
import sqlalchemy
import pandas as pd


class MysqlClient:

    def __init__(self, db_type):

        conn_str = mysql_secret.get_conn_str(db_type)
        self.engine = sqlalchemy.create_engine(conn_str)

    def pull(self, query_str):

        result = pd.read_sql(sql=query_str, con=self.engine)
        return result

    def push(self, table_name, df):

        df.to_sql(name=table_name, con=self.engine, if_exists='append', index=False)

    def execute(self, query_str):

        with self.engine.connect() as con:
            con.execute(query_str)


if __name__ == '__main__':
    sql = MysqlClient(db_type='dev')
    test_result = sql.pull('SELECT 1')
    print(test_result)
