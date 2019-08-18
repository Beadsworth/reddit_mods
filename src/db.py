import mysql
import mysql_secret
import ssh_secret


class DBConnection(mysql.MySQLClient):

    databases = {
        'dev': 'reddit_mods_dev',
        'prod': 'reddit_mods_prod'
    }

    @classmethod
    def get_db_conn(cls, mode='dev', remote=False):

        if remote:
            return cls.from_ssh_tunnel(
                remote_ip=ssh_secret.remote_ip,
                ssh_port=ssh_secret.ssh_port,
                ssh_username=ssh_secret.ssh_username,
                ssh_password=ssh_secret.ssh_password,
                mysql_username=mysql_secret.username,
                mysql_password=mysql_secret.password,
                mysql_host=mysql_secret.host,
                mysql_port=mysql_secret.port,
                mysql_database=cls.databases[mode]
            )

        else:

            return cls(
                mysql_username=mysql_secret.username,
                mysql_password=mysql_secret.password,
                mysql_host=mysql_secret.host,
                mysql_port=mysql_secret.port,
                mysql_database=cls.databases[mode]
            )

    def get_last_scan_id(self):
        query_str = \
            """
            SELECT id
            FROM reddit_mods_dev.scans
            ORDER BY id DESC
            LIMIT 1
            """

        result = self.pull(query_str=query_str)
        last_id = int(result['id'].loc[0])

        return last_id

    def get_top_subs_from_scan_id(self, scan_id):
        query_str = \
            """
            SELECT
                 id AS top_subreddits_id
                ,subreddit_id
            FROM reddit_mods_dev.top_subreddits
            WHERE scan_id = {scan_id}
            ORDER BY id
            """.format(scan_id=scan_id)

        result = self.pull(query_str=query_str)
        return result

    def get_top_mods_from_scan_id(self, scan_id):
        query_str = \
            """
            SELECT DISTINCT
                 t.scan_id
                ,m.id AS mod_id
                ,t.moderator_name
            FROM reddit_mods_dev.top_mods AS t
            JOIN reddit_mods_dev.active_moderators AS m
                ON m.moderator_id = t.moderator_id
            WHERE t.scan_id = {scan_id}
            ORDER BY t.scan_id, t.moderator_name
            """.format(scan_id=scan_id)

        result = self.pull(query_str=query_str)
        return result

    def get_exhaustive_subs_from_scan_id(self, scan_id):
        query_str = \
            """
            SELECT DISTINCT
                 u.scan_id
                ,u.subreddit_display_name
                ,s.subreddit_id
            FROM user_modded_subs AS u
            JOIN subreddits AS s
                ON s.subreddit_display_name = u.subreddit_display_name
            WHERE u.scan_id = {scan_id}
            ORDER BY u.scan_id, s.subreddit_id
            """.format(scan_id=scan_id)

        result = self.pull(query_str=query_str)
        return result

    def get_missing_sub_ids_from_scan(self, scan_id):
        query_str = \
            """
            SELECT DISTINCT
                u.subreddit_display_name
            FROM user_modded_subs AS u
            LEFT JOIN subreddits AS n
                ON n.subreddit_display_name = u.subreddit_display_name
            WHERE u.scan_id = {scan_id}
                AND n.subreddit_id IS NULL
            ORDER BY u.subreddit_display_name
            """.format(scan_id=scan_id)

        result = self.pull(query_str=query_str)
        return result

    def get_missing_mod_ids_from_scan(self, scan_id):
        query_str = \
            """
            SELECT DISTINCT
                 t.moderator_id
                ,t.moderator_name
            FROM top_mods AS t
            LEFT JOIN moderators AS m
                ON m.moderator_id = t.moderator_id
            WHERE t.scan_id = {scan_id}
                AND m.moderator_id IS NULL
            ORDER BY t.moderator_id, t.moderator_name
            """.format(scan_id=scan_id)

        result = self.pull(query_str=query_str)
        return result


if __name__ == '__main__':

    print("starting script...")

    db_connection = DBConnection.get_db_conn(mode='dev', remote=False)

    with db_connection as sql:

        test_result = sql.get_last_scan_id()
        print(test_result)

    print("done!")
