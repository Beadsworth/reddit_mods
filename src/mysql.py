import sqlalchemy
import pandas as pd
import urllib.parse
import sshtunnel


class MySQLClient:

    def __init__(self, mysql_username, mysql_password, mysql_host, mysql_port, mysql_database, ssh_server=None):

        self.ssh_server = ssh_server

        self.mysql_username = mysql_username
        self.mysql_password = mysql_password
        self.mysql_host = mysql_host
        self.mysql_port = mysql_port
        self.mysql_database = mysql_database

        self.engine = None

    @staticmethod
    def get_conn_str(username, password, host, port, database):

        # create connection str
        # dialect+driver://username:password@host:port/database
        conn_str = 'mysql+pymysql://{username}:{password}@{host}:{port}/{database}' \
            .format(username=username,
                    password=urllib.parse.quote_plus(password),
                    host=host,
                    port=port,
                    database=database)

        return conn_str

    def open_db(self):
        host = self.mysql_host
        port = self.mysql_port

        if self.ssh_server:
            self.ssh_server.start()
            host = self.ssh_server.local_bind_host
            port = self.ssh_server.local_bind_port

        conn_str = self.get_conn_str(username=self.mysql_username,
                                     password=self.mysql_password,
                                     host=host,
                                     port=port,
                                     database=self.mysql_database)

        self.engine = sqlalchemy.create_engine(conn_str)

    def close_db(self):
        self.engine.dispose()
        if self.ssh_server:
            self.ssh_server.stop()

    def __enter__(self):
        self.open_db()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # make sure the dbconnection gets closed
        self.close_db()

    def pull(self, query_str):

        result = pd.read_sql(sql=query_str, con=self.engine)
        return result

    def push(self, table_name, df):

        df.to_sql(name=table_name, con=self.engine, if_exists='append', index=False)

    def execute(self, query_str):

        with self.engine.connect() as con:
            con.execute(query_str)

    @classmethod
    def from_ssh_tunnel(cls, remote_ip, ssh_port, ssh_username, ssh_password,
                        mysql_username, mysql_password, mysql_host, mysql_port, mysql_database):

        ssh_server = sshtunnel.SSHTunnelForwarder(ssh_address_or_host=(remote_ip, ssh_port),
                                                  ssh_username=ssh_username,
                                                  ssh_password=ssh_password,
                                                  remote_bind_address=(mysql_host, mysql_port))

        return cls(mysql_username=mysql_username,
                   mysql_password=mysql_password,
                   mysql_host=mysql_host,
                   mysql_port=mysql_port,
                   mysql_database=mysql_database,
                   ssh_server=ssh_server)


if __name__ == '__main__':
    import mysql_secret
    import ssh_secret

    print("starting script...")

    db_connection = MySQLClient.from_ssh_tunnel(remote_ip=ssh_secret.remote_ip,
                                                ssh_port=ssh_secret.ssh_port,
                                                ssh_username=ssh_secret.ssh_username,
                                                ssh_password=ssh_secret.ssh_password,
                                                mysql_username=mysql_secret.username,
                                                mysql_password=mysql_secret.password,
                                                mysql_host=mysql_secret.host,
                                                mysql_port=mysql_secret.port,
                                                mysql_database='reddit_mods_dev')

    with db_connection as sql:

        test_result = sql.pull('SELECT * FROM subreddits ORDER BY log_date DESC LIMIT 10')
        print(test_result)

    print("done!")
