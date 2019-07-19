import praw
import reddit_secret
import requests
import random
import pandas as pd
import datetime as dt
import re
import time
import src.mysql as sql
import prawcore

try:
    from BeautifulSoup import BeautifulSoup
except ImportError:
    from bs4 import BeautifulSoup

def get_user_agent_headers():
    user_agent_list = [
       #Chrome
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
        #Firefox
        'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
        'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
        'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)'
    ]
    url = 'https://httpbin.org/user-agent'
    #Lets make 5 requests and see what user agents are used

    #Pick a random user agent
    user_agent = random.choice(user_agent_list)
    #Set the headers
    headers = {'User-Agent': user_agent}

    return headers


# def get_users_mod_list(user):
#     """
#     user is username only, no leading 'u/'
#     return list of subreddits, no leading 'r/'
#     """
#
#     # css class for moderated subreddits -- found on a redditor's "user" page
#     modded_subreddits_class = 'ylup29-4 dllHsI'
#
#     modded_subreddits = []
#     user_page = 'http://www.reddit.com/user/' + user
#
#     try:
#         # request user page
#         response = requests.get(user_page, headers=get_user_agent_headers())
#
#         if response.status_code != 200:
#             raise RuntimeError("request failed!")
#
#         # html_test_file_path = r'/home/jmb/PycharmProjects/reddit_mods/src/test_files/test_moderator_page.html'
#
#         # with open(html_test_file_path, 'r') as f:
#         #     response = f.read()
#
#         # parse html
#         content = response.content
#         parsed_html = BeautifulSoup(content, features='lxml')
#         modded_subreddits = [x.text.split('r/')[-1] for x in parsed_html.find_all("a", class_=modded_subreddits_class)]
#
#         if len(modded_subreddits)==0:
#             print("oops")
#
#     finally:
#         return modded_subreddits


class Reddit:

    url_pattern = re.compile('/?(.+?)/(.+?)/?$')

    def __init__(self):
        self.reddit_client = praw.Reddit(user_agent=reddit_secret.user_agent,
                                         client_id=reddit_secret.client_id,
                                         client_secret=reddit_secret.client_secret)

        self.sql_client = sql.MysqlClient()

    def get_sub_id_from_name(self, subreddit_name):

        print(subreddit_name)

        try:
            sub_id = self.reddit_client.subreddit(subreddit_name).id

        except (prawcore.exceptions.Forbidden, prawcore.exceptions.NotFound) as e:
            print('could not find id for /r/'+subreddit_name)
            sub_id = None

        return sub_id

    def get_last_scan_id(self):

        query_str =\
            """
            SELECT id
            FROM reddit_mods_dev.scans
            ORDER BY id DESC
            LIMIT 1
            """

        result = self.sql_client.pull(query_str=query_str)
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

        result = self.sql_client.pull(query_str=query_str)
        return result

    def get_top_mods_from_scan_id(self, scan_id):

        query_str = \
            """
            SELECT 
                 s.scan_id
                ,m.moderator_name
                ,m.id AS top_mods_id
            FROM reddit_mods_dev.top_mods AS m
            JOIN reddit_mods_dev.top_subreddits AS s
                ON s.id = m.top_subreddits_id
            WHERE s.scan_id = {scan_id}
            ORDER BY s.scan_id, m.id
            """.format(scan_id=scan_id)

        result = self.sql_client.pull(query_str=query_str)
        return result

    def get_missing_sub_ids_from_scan(self, scan_id):

        query_str = \
            """
            SELECT DISTINCT
                u.subreddit_display_name
            FROM user_modded_subs AS u
            JOIN top_mods AS m
                ON m.id = u.top_mods_id
            JOIN top_subreddits AS s
                ON s.id = m.top_subreddits_id
            LEFT JOIN subreddit_names AS n
                ON n.subreddit_display_name = u.subreddit_display_name
            WHERE s.scan_id = {scan_id}
                AND n.subreddit_id IS NULL
            ORDER BY u.subreddit_display_name
            LIMIT 5000
            """.format(scan_id=scan_id)

        result = self.sql_client.pull(query_str=query_str)
        return result

    def store_missing_sub_ids_for_scan(self, scan_id):

        subreddit_names = self.get_missing_sub_ids_from_scan(scan_id=scan_id)
        subreddit_names['subreddit_id'] = subreddit_names['subreddit_display_name']\
            .apply(lambda x: reddit.get_sub_id_from_name(x))
        subreddit_names['log_date'] = dt.datetime.now()

        # public subreddits with ids that are found
        found_subreddit_names = subreddit_names[subreddit_names['subreddit_id'].notnull()]

        # push
        reddit.sql_client.push('subreddit_names', found_subreddit_names)

    def get_top_subreddits(self, count):
        """
        :param count: number of subreddits to fetch
        :return: DataFrame of subreddits, ranked on popularity
        """

        target_keys = [
            'id',
            'display_name',
        ]

        subs = []
        for i, sub in enumerate(self.reddit_client.subreddits.popular(limit=count)):

            sub_dict = {key: getattr(sub, key) for key in target_keys}
            sub_dict['subreddit_current_rank'] = i + 1
            subs.append(sub_dict)

        df = pd.DataFrame(subs)
        # fix ids
        df['id'] = df['id'].apply(lambda x: 't5_'+x)

        # fix column names
        df.rename(index=str, columns={'id': 'subreddit_id', 'display_name': 'subreddit_display_name'}, inplace=True)

        return df.set_index('subreddit_current_rank', drop=False).sort_index(ascending=True)

    def get_subreddits_moderators(self, subreddit_ids):
        """
        :param subreddit_ids: list of subreddit ids to fetch
        :return: DataFrame mod list all requested subreddits
        """

        subs = self.reddit_client.info(subreddit_ids)
        mods = [{'subreddit_id': sub.id, 'moderator_name': mod.name, 'moderator_id': mod.id}
                for sub in subs for mod in sub.moderator()]

        df = pd.DataFrame(mods)
        # fix ids
        df['subreddit_id'] = df['subreddit_id'].apply(lambda x: 't5_' + x)

        return df.sort_values(['subreddit_id', 'moderator_name']).reset_index(drop=True)

    def get_user_mod_list(self, user):

        """
        :param user: user to fetch
        :return: df with subreddits moderated by user
        """

        user_page = 'http://ps.reddit.com/user/' + user
        # bypass nsfw age confirmation
        cookies = {'over18': '1'}

        modded_subreddits = []

        # request user page
        time.sleep(2)
        response = requests.get(user_page, headers=get_user_agent_headers(), cookies=cookies)

        if response.status_code != 200:
            raise RuntimeError("request failed: {}".format(response.status_code))

        # parse html
        content = response.content
        parsed_html = BeautifulSoup(content, features='lxml')

        modded_subreddits = []
        for y in parsed_html.find('ul', id='side-mod-list').contents:
            subreddit_name = y.contents[0]['href'].split('/r/')[-1].split('/')[0]
            page_type, subreddit_name = self.url_pattern.findall(y.contents[0]['href'])[0]

            # if it's a subreddit
            if page_type == 'r':

                # subreddit_id = self.get_sub_id_from_name(subreddit_name)
                mod_dist = {
                    'user': user,
                    'subreddit_display_name': subreddit_name
                }

                modded_subreddits.append(mod_dist)

        df = pd.DataFrame(modded_subreddits)
        # fix ids
        # df['subreddit_id'] = df['subreddit_id'].apply(lambda x: 't5_' + x)
        # return modded_subreddits
        return df.sort_values(['user', 'subreddit_display_name']).reset_index(drop=True)

    def get_users_mod_list(self, users):
        """
        :param users: list of users
        :return: DataFrame of users and their moderated subs
        """

        all_mod_lists = []
        for user in users:
            mod_list = self.get_user_mod_list(user)
            all_mod_lists.extend(mod_list)

        return pd.DataFrame(all_mod_lists).set_index('user').sort_values(['user', 'subreddit_name'])

    # def get_users_mod_list_with_ids(self, users):
    #
    #     users_mod_list = self.get_users_mod_list(users)
    #
    #     unique_subreddits = users_mod_list['subreddit_name'].unique().tolist()
    #     sub_ids = pd.DataFrame(
    #         [{'subreddit_name': subreddit_name, 'subreddit_id': self.get_sub_id_from_name(subreddit_name)}
    #          for subreddit_name in unique_subreddits]
    #     )
    #
    #
    #     print(sub_ids)

    def get_subreddits_info(self, subreddit_ids):
        """
        :param subreddit_ids: list of subreddit_ids
        :return: dictionary of info for all subreddits requested
        """

        # TODO check out subreddit.traffic()

        target_keys = [
            'id',
            'display_name',
            'url',
            'over18',
            'lang',
            'active_user_count',
            'subscribers',
            'subreddit_type'
        ]

        subs = self.reddit_client.info(subreddit_ids)
        subs_info = [{key: getattr(sub, key) for key in target_keys} for sub in subs]

        df = pd.DataFrame(subs_info)
        # fix ids
        df['id'] = df['id'].apply(lambda x: 't5_' + x)

        return df.set_index('id').sort_values('display_name')


if __name__ == '__main__':

    print("starting script @{} ...".format(dt.datetime.now()))

    one_hour = 3600
    pause_time = 24 * one_hour

    for i in range(100):

        print("iteration:", i)

        reddit = Reddit()

        # print(reddit.get_sub_id_from_name('AskReddit'))
        # print(reddit.get_sub_id_from_name('ChristmasStory'))

        scan_time = dt.datetime.now()
        scan_df = pd.DataFrame([{'scan_start_date': scan_time}])
        reddit.sql_client.push('scans', scan_df)

        scan_id = reddit.get_last_scan_id()

        # # if subreddit_id is missing from db, get it
        # start_time = dt.datetime.now()
        # reddit.store_missing_sub_ids_for_scan(scan_id=125)
        #
        # print(dt.datetime.now() - start_time)


        top_subs = reddit.get_top_subreddits(10)
        # add scan_id
        top_subs['scan_id'] = scan_id
        # add log date
        top_subs['log_date'] = dt.datetime.now()
        # push
        reddit.sql_client.push('top_subreddits', top_subs)
        # print(top_subs)

        top_sub_ids = reddit.get_top_subs_from_scan_id(scan_id=scan_id)
        top_sub_ids_list = top_sub_ids['subreddit_id'].unique().tolist()
        top_mods = reddit.get_subreddits_moderators(top_sub_ids_list)
        # join top_subreddit_ids
        top_mods = pd.merge(left=top_sub_ids, right=top_mods, how='left', on='subreddit_id')
        # add log date
        top_mods['log_date'] = dt.datetime.now()
        # push
        reddit.sql_client.push('top_mods', top_mods)
        # print(top_mods)

        top_mod_from_db = reddit.get_top_mods_from_scan_id(scan_id)

        for index, row in top_mod_from_db.iterrows():
            print(index)
            mod_list_df = reddit.get_user_mod_list(row['moderator_name'])
            mod_list_df['log_date'] = dt.datetime.now()
            mod_list_df['top_mods_id'] = row['top_mods_id']
            reddit.sql_client.push('user_modded_subs', mod_list_df)

        # if subreddit_id is missing from db, get it
        reddit.store_missing_sub_ids_for_scan(scan_id=scan_id)

        # sleep
        for j in range(pause_time):

            remaining_time = pause_time - j

            print("next iteration in {seconds} seconds".format(seconds=remaining_time), end='')
            time.sleep(1)
            print("\r" + " " * 100, end='\r')

    print("done @{}!".format(dt.datetime.now()))
