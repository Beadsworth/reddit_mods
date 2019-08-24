import praw
import prawcore
import reddit_secret
import requests
import random
import pandas as pd
import datetime as dt
import re
import time
import db
import tqdm
import json


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

    # Pick a random user agent
    user_agent = random.choice(user_agent_list)
    # Set the headers
    headers = {'User-Agent': user_agent}

    return headers


class RedditModData:

    valid_modes = 'dev', 'prod'
    url_pattern = re.compile('/?(.+?)/(.+?)/?$')

    def __init__(self, mode, remote):
        assert mode in self.valid_modes, "invalid mode given: {mode}".format(mode=mode)
        self.reddit_client = praw.Reddit(user_agent=reddit_secret.user_agent,
                                         client_id=reddit_secret.client_id,
                                         client_secret=reddit_secret.client_secret)

        self.db_conn = db.DBConnection.get_db_conn(mode=mode, remote=remote)

    def get_sub_json_from_name_from_web(self, scan_id, subreddit_name):

        time.sleep(2)

        # subreddit info page
        subreddit_page = 'http://ps.reddit.com/r/{}/about.json'.format(subreddit_name)
        # bypass nsfw age confirmation and quarantines
        cookies = {'over18': '1', '_options': '{%22pref_quarantine_optin%22:true}'}
        response = requests.get(subreddit_page, headers=get_user_agent_headers(), cookies=cookies)

        sub_json = None

        if response.status_code == 200:

            # parse json
            sub_json = json.loads(response.content)

        # cannot find subreddit
        else:
            # print('error code {error}: cannot access subreddit {subreddit_name}'
            #       .format(error=response.status_code, subreddit_name=subreddit_name))
            # log error in db
            subreddit_errors = pd.DataFrame([{'scan_id': scan_id, 'subreddit_display_name': subreddit_name,
                                              'error_code': response.status_code, 'log_date': dt.datetime.now()}])
            self.db_conn.push('subreddit_errors',
                              subreddit_errors[['scan_id', 'subreddit_display_name', 'error_code', 'log_date']])

        return sub_json

    def get_sub_id_from_name(self, scan_id, subreddit_name):

        sub_id = None

        try:
            sub_id = 't5_' + str(self.reddit_client.subreddit(subreddit_name).id)

        # TODO clean up
        except (prawcore.exceptions.Forbidden, prawcore.exceptions.NotFound, prawcore.exceptions.Redirect) as e:

            try:
                sub_json = self.get_sub_json_from_name_from_web(scan_id=scan_id, subreddit_name=subreddit_name)
                sub_id = sub_json['data']['name']

            except (KeyError, TypeError) as e:
                print('could not find id for /r/'+subreddit_name)

        return sub_id

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

        sub_count = len(subreddit_ids)
        subs = self.reddit_client.info(subreddit_ids)
        mods = [{'subreddit_id': sub.id, 'moderator_name': mod.name, 'moderator_id': mod.id}
                for sub in tqdm.tqdm(subs, total=sub_count, unit='subreddit') for mod in sub.moderator()]

        df = pd.DataFrame(mods)
        # fix ids
        df['subreddit_id'] = df['subreddit_id'].apply(lambda x: 't5_' + x)

        return df[df['moderator_id'].notnull()].sort_values(['subreddit_id', 'moderator_name']).reset_index(drop=True)

    def get_user_mod_list(self, moderator_name, mod_id, scan_id):

        """
        :param moderator_name: user to fetch
        :return: df with subreddits moderated by user
        """

        time.sleep(2)

        # request user page
        user_page = 'http://ps.reddit.com/user/' + moderator_name
        # bypass nsfw age confirmation and quarantines
        cookies = {'over18': '1', '_options': '{%22pref_quarantine_optin%22:true}'}
        response = requests.get(user_page, headers=get_user_agent_headers(), cookies=cookies)
        modded_subreddits = []

        # TODO react to suspended accounts
        if response.status_code == 200:

            # parse html
            content = response.content
            parsed_html = BeautifulSoup(content, features='lxml')
            found = parsed_html.find('ul', id='side-mod-list')

            if found:
                # add subreddits to modded_subreddits list
                for y in found.contents:
                    subreddit_name = y.contents[0]['href'].split('/r/')[-1].split('/')[0]
                    page_type, subreddit_name = self.url_pattern.findall(y.contents[0]['href'])[0]

                    # if it's a subreddit
                    if page_type == 'r':
                        # subreddit_id = self.get_sub_id_from_name(subreddit_name)
                        mod_dict = {
                            'scan_id': scan_id,
                            'mod_id': mod_id,
                            'subreddit_display_name': subreddit_name
                        }

                        modded_subreddits.append(mod_dict)
            else:
                # print('couldn\'t find subreddits on user\'s page: {user}'.format(user=user))
                mod_dict = {
                    'scan_id': scan_id,
                    'mod_id': mod_id,
                    'subreddit_display_name': None
                }

                modded_subreddits.append(mod_dict)

        # suspended account
        else:
            # print('error code {error}: cannot access account for {user}'.format(error=response.status_code, user=moderator_name))
            # log error in db
            mod_errors = pd.DataFrame([{'scan_id': scan_id, 'mod_id': mod_id, 'error_code': response.status_code,
                                       'log_date': dt.datetime.now()}])
            self.db_conn.push('moderator_errors', mod_errors[['scan_id', 'mod_id', 'error_code', 'log_date']])

            mod_dict = {
                'scan_id': scan_id,
                'mod_id': mod_id,
                'subreddit_display_name': None
            }

            modded_subreddits.append(mod_dict)

        df = pd.DataFrame(modded_subreddits)
        return df.sort_values(['scan_id', 'mod_id', 'subreddit_display_name']).reset_index(drop=True)

    def get_subreddits_info(self, subreddit_ids):
        """
        :param subreddit_ids: list of subreddit_ids
        :return: dictionary of info for all subreddits requested
        """

        # TODO check out subreddit.traffic()

        target_keys = [
            'name',
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
        # df['id'] = df['id'].apply(lambda x: 't5_' + x)

        return df.rename(columns={'name': 'subreddit_id'}).sort_values('display_name').set_index('subreddit_id',
                                                                                                 drop=True)

    def checkout_id(self):
        scan_time = dt.datetime.now()
        scan_df = pd.DataFrame([{'scan_start_date': scan_time}])
        self.db_conn.push('scans', scan_df)

        scan_id = self.db_conn.get_last_scan_id()
        print("checked out scan_id: {scan_id}".format(scan_id=scan_id))
        return scan_id

    def store_top_subs(self, scan_id, sub_count):

        reddit_top_subs = self.get_top_subreddits(sub_count)
        # add scan_id
        reddit_top_subs['scan_id'] = scan_id
        # add log date
        reddit_top_subs['log_date'] = dt.datetime.now()
        # push
        self.db_conn.push('top_subreddits', reddit_top_subs)
        # print(top_subs)

    def store_top_mods(self, scan_id):
        db_top_subs = self.db_conn.get_top_subs_from_scan_id(scan_id=scan_id)

        top_sub_ids_list = db_top_subs['subreddit_id'].unique().tolist()
        reddit_top_mods = self.get_subreddits_moderators(top_sub_ids_list)

        # join top_subreddit_ids
        # reddit_top_mods = pd.merge(left=db_top_subs, right=reddit_top_mods, how='left', on='subreddit_id')

        # add log date & scan_id
        reddit_top_mods['log_date'] = dt.datetime.now()
        reddit_top_mods['scan_id'] = scan_id
        # push
        self.db_conn.push('top_mods', reddit_top_mods[['moderator_name', 'moderator_id', 'subreddit_id', 'log_date',
                                                      'scan_id']])

    def store_user_modded_subs(self, scan_id):
        db_top_mods = self.db_conn.get_top_mods_from_scan_id(scan_id)

        mod_count = len(db_top_mods)

        for index, row in tqdm.tqdm(db_top_mods.iterrows(), total=mod_count, unit='moderator'):
            # print("fetching subs for moderator {current} of {total}".format(current=index + 1, total=mod_count))
            reddit_user_modded_subs = self.get_user_mod_list(moderator_name=row['moderator_name'], mod_id=row['mod_id'],
                                                             scan_id=scan_id)
            # TODO consider moving to inside get_user_mod_list()
            # push mod list has at least one subreddit
            if len(reddit_user_modded_subs[reddit_user_modded_subs['subreddit_display_name'].notnull()]) > 0:

                reddit_user_modded_subs['log_date'] = dt.datetime.now()
                self.db_conn.push('user_modded_subs', reddit_user_modded_subs[['scan_id', 'mod_id',
                                                                              'subreddit_display_name', 'log_date']])

    def store_missing_mod_ids_for_scan(self, scan_id):
        # TODO create ignore list in db

        new_mods = self.db_conn.get_missing_mod_ids_from_scan(scan_id=scan_id)
        new_mods['log_date'] = dt.datetime.now()

        # push
        self.db_conn.push('moderators', new_mods[['moderator_id', 'moderator_name', 'log_date']])

    def store_missing_sub_ids_for_scan(self, scan_id):
        # TODO create ignore list in db

        subreddit_names = self.db_conn.get_missing_sub_ids_from_scan(scan_id=scan_id)
        subreddit_names['subreddit_id'] = subreddit_names['subreddit_display_name']\
            .apply(lambda x: self.get_sub_id_from_name(scan_id=scan_id, subreddit_name=x))
        subreddit_names['log_date'] = dt.datetime.now()

        # public subreddits with ids that are found
        found_subreddit_names = subreddit_names[subreddit_names['subreddit_id'].notnull()]

        # push
        self.db_conn.push('subreddits', found_subreddit_names[['subreddit_id', 'subreddit_display_name', 'log_date']])

    def store_exhaustive_subs_info(self, scan_id):
        db_exhaustive_subs = self.db_conn.get_exhaustive_subs_from_scan_id(scan_id=scan_id)

        reddit_exhaustive_subs_info = self.get_subreddits_info(db_exhaustive_subs['subreddit_id'].tolist())
        reddit_exhaustive_subs_info['scan_id'] = scan_id
        reddit_exhaustive_subs_info['log_date'] = dt.datetime.now()
        reddit_exhaustive_subs_info.reset_index(drop=False, inplace=True)

        # push
        self.db_conn.push(table_name='subreddit_details', df=reddit_exhaustive_subs_info)

    def complete_scan(self, scan_id):

        completion_time = dt.datetime.now()
        completion_df = pd.DataFrame([{'scan_id': scan_id, 'scan_end_date': completion_time}])
        self.db_conn.push('completions', completion_df)

    def perform_one_scan(self, sub_count=1):

        # perform all actions within the context of a database connection and possible ssh tunnel
        with self.db_conn as conn:
            # checkout a scan_id
            print("checking out a scan id...")
            current_scan_id = self.checkout_id()

            # get the most popular subreddits
            print("recording most popular subreddits...")
            self.store_top_subs(scan_id=current_scan_id, sub_count=sub_count)

            # for each top subreddit, get all mods
            print("recording top moderators...")
            self.store_top_mods(scan_id=current_scan_id)

            # if new top-mod is found, store him in the moderators table
            print("storing new moderators...")
            self.store_missing_mod_ids_for_scan(scan_id=current_scan_id)

            # for each top mod, get all other subreddits moderated by that mod
            print("storing each moderator's modded subs...")
            self.store_user_modded_subs(scan_id=current_scan_id)

            # if subreddit_id is missing from db, get it
            print("storing new subreddit ids...")
            self.store_missing_sub_ids_for_scan(scan_id=current_scan_id)

            # for each subreddit modded by a top-mod, get subreddit info
            print("storing all subreddit information...")
            self.store_exhaustive_subs_info(scan_id=current_scan_id)

            # save scan completion in db
            print("finishing up...")
            self.complete_scan(scan_id=current_scan_id)
