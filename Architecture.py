"""

Auth setup and Token Generation.

This file contains methods for Authentication to FB API and feed contents.
"""
import json
from dateutil import parser
import datetime
import requests
import time
import facebook
import pandas as pd
import re


class FBCrawler(object):
    """Intializing the url to get Access Token."""

    def __init__(self):
        """Baisc Settings."""
        self.base_tok_url = 'https://graph.facebook.com/oauth/access_token?'

    def fb_auth(self, url):
        """Auth to FB API using dynamic Access Token."""
        # Generating Access Token
        flag = True
        try:
            req = requests.get(self.base_tok_url + url)
            access_token = req.text.split('=')[1]
            # Getting authentication from FACEBOOK GRAPH API.
            graph = facebook.GraphAPI(access_token)
        except:
            time.sleep(5)
            try:
                req = requests.get(self.base_tok_url + url)
                access_token = req.text.split('=')[1]
                # Getting authentication from FACEBOOK GRAPH API.
                graph = facebook.GraphAPI(access_token)
            except:
                graph = False
                flag = False
        return flag, graph

    def get_updated_posts(self, graph, page, since, params):
        """Function to get Latest posts."""
        try:
            posts = graph.get_object(str(page) + '/posts',
                                     fields=params,
                                     since=since)['data']
        except:
            posts = {}
        return posts

    def get_total_likes(self, graph, post, back_off, likes, proxy=False,
                        _http=False):
        """Function to get total likes."""
        total_likes = 0

        if back_off <= 200 and back_off > 0:
            try:
                time.sleep(1)
                res = graph.get_object(str(post) + '/likes', summary=True,
                                       filter='stream', limit=back_off)
                time.sleep(1)
            except:
                try:
                    time.sleep(1)
                    res = graph.get_object(str(post) + '/likes', summary=True,
                                           filter='stream', limit=back_off)
                    time.sleep(1)
                except:
                    res = {}
            likes.extend(res.get('data', []))

        elif back_off > 200 and back_off > 0:
            try:
                res = graph.get_object(str(post) + '/likes', summary=True,
                                       filter='stream', limit=200)
            except:
                try:
                    time.sleep(2)
                    res = graph.get_object(str(post) + '/likes', summary=True,
                                           filter='stream', limit=200)
                    time.sleep(1)
                except:
                    res = {}

            likes = res.get('data', [])
            total_likes = len(likes)
            remaining = 0

            while 'paging' in res and 'next' in res.get('paging', {}):
                status_code = 200
                remaining = (back_off - total_likes)

                if remaining > 0:
                    if proxy and _http:
                        try:
                            time.sleep(2)
                            _likes = requests.get(
                                res.get('paging', {}).get('next'),
                                proxies={'http': 'http://%s' % _http})
                            time.sleep(2)
                        except:
                            status_code = 404
                            _likes = {}
                    else:
                        try:
                            time.sleep(2)
                            _likes = requests.get(
                                res.get('paging', {}).get('next'))
                            time.sleep(2)
                        except:
                            try:
                                time.sleep(2)
                                _likes = requests.get(
                                    res.get('paging', {}).get('next'))
                                time.sleep(2)
                            except:
                                status_code = 404
                                _likes = {}

                    if status_code == 200:
                        try:
                            _likes = _likes.text
                            res = json.loads(_likes)
                        except:
                            res = {}

                        total_likes = total_likes + len(res.get('data', []))

                        if res.get('data'):
                            if total_likes > back_off:
                                total_likes = back_off
                                res['data'] = res.get('data', [])[:(remaining)]

                            if total_likes <= back_off:
                                likes.extend(res.get('data', []))
                else:
                    break
        else:
            likes = []

        return likes[:(back_off)]

    def get_total_comments(self, graph, posts, since, proxy=False,
                           http_proxy=False):
        """Function to get total comments."""
        data_com = []
        since = parser.parse(since)
        for post in posts:
            try:
                time.sleep(1)
                res = graph.get_object(str(post) + '/comments', summary=True,
                                       orderby='stream', limit=200)
                time.sleep(1)
            except:
                time.sleep(1)
                try:
                    res = graph.get_object(str(post) + '/comments',
                                           summary=True,
                                           orderby='nonchronicle', limit=200)
                except:
                    res = {}
                time.sleep(1)

            int_list = []

            if res.get('data', {}):
                for _com in res.get('data'):
                    try:
                        _time = _com.get('created_time', '').replace(
                            '+0000', '')
                        # Converting str to date object.
                        _time = parser.parse(
                            _time) + datetime.timedelta(hours=5, minutes=30)
                    except:
                        _time = since

                    if _time > since:
                        int_list.append(_com)
                    # else:
                    #     break
                data_com.append(int_list)
                loop_flag = False

                while 'paging' in res and 'next' in res.get('paging'):
                    # Adding PROXY if applicable #
                    status_code = 200

                    if proxy and http_proxy:
                        try:
                            _coment = requests.get(
                                res('paging', {}).get('next'),
                                proxies={'http': 'http://%s' % http_proxy})
                            time.sleep(2)
                        except:
                            status_code = 404
                            _coment = {}
                            time.sleep(2)
                    else:
                        try:
                            time.sleep(2)
                            _coment = requests.get(
                                res.get('paging', {}).get('next'))
                            time.sleep(2)
                        except:
                            try:
                                time.sleep(2)
                                _coment = requests.get(
                                    res.get('paging', {}).get('next'))
                                time.sleep(2)
                            except:
                                status_code = 404
                                _coment = {}

                    if status_code == 200:
                        try:
                            _coment = _coment.text
                            res = json.loads(_coment)
                        except:
                            res = {}
                        loop_list = []
                        if res.get('data'):
                            for _ncom in res.get('data'):
                                try:
                                    _ntime = _com.get(
                                        'created_time', '').replace(
                                        '+0000', '')
                                    # Converting str to date object.
                                    _ntime = parser.parse(
                                        _ntime) + datetime.timedelta(
                                        hours=5, minutes=30)
                                except:
                                    _ntime = since

                                if _ntime > since:
                                    loop_list.append(_ncom)
                                else:
                                    loop_flag = True
                                    break
                        data_com.append(loop_list)
                    else:
                        loop_flag = True
                    if loop_flag:
                        break

        return data_com

    def get_summary(self, graph, _id, ptype):
        """A Generic function to get Number of comments/likes for a POST."""
        # Setting flag in Posts exists.
        consider = True
        try:
            met_type = graph.get_object(str(_id) + '/' + str(ptype),
                                        summary=True)
        except:
            # If posts does not exists or Network Latency.
            consider = False
            met_type = {}

        try:
            met_type = met_type.get('summary', {})
            if len(met_type):
                met_type = met_type.get('total_count', 0)
            else:
                met_type = 0
                consider = False
        except:
            consider = False
            met_type = 0
        return met_type, consider

    def get_posts_stats(self, graph, _id):
        """Function to get latest stats of a POST."""
        shares_consider = True
        try:
            shares = graph.get_object(str(_id),
                                      summary=True, filter='stream')
            time.sleep(1)
        except:
            shares_consider = False
            shares = {}
        shares = shares.get('shares', {}).get('count', 0)
        time.sleep(1)

        likes, likes_consider = FBCrawler.get_summary(graph, _id, 'likes')
        likes = int(float(str(likes)))
        time.sleep(1)

        comments, comments_consider = FBCrawler.get_summary(
            graph, _id, 'comments')
        comments = int(float(str(comments)))
        time.sleep(1)
        _stats = {'shares': shares, 'likes': likes, 'comments': comments,
                  'shares_consider': shares_consider,
                  'likes_consider': likes_consider,
                  'comments_consider': comments_consider}
        return _stats


class FBProcess(object):
    """Class to process FACEBOOK DATA."""

    def __init__(self, url):
        """Intialization constructor."""
        self.AuthArch = FBCrawler()
        self.url = url

    def date_conv(self, _date):
        """Function to convert datetime object into proper format."""
        _date = parser.parse(_date)
        _delta = datetime.timedelta(hours=5, minutes=30)
        _date = _date + _delta
        _date = _date.strftime('%d-%m-%Y %H:%M')
        return _date

    def get_likes(self, likes_df, all_likes, **kwargs):
        """

        Function to extract User ID of users those who are liking a.

        particular post.
        """
        # ------------------ LOOKING FOR PROXY OPTIONS ---------------------- #
        proxy = False
        http_proxy = False
        if kwargs:
            if kwargs.get('proxy', False) and kwargs.get('HTTP_PROXY'):
                proxy = kwargs.get('proxy', False)
                http_proxy = kwargs.get('HTTP_PROXY')
        # ------------------------------------------------------------------- #
        time.sleep(1)
        auth_flag, graph = self.AuthArch.fb_auth(self.url)
        time.sleep(1)

        data_likes = []
        scrapped_at = datetime.datetime.now()
        scrapped_at = scrapped_at.strftime('%d-%m-%Y %H:%M')

        if type(likes_df) is pd.DataFrame:
            likes_df['scrapped_date'] = scrapped_at

            for index, _data in likes_df.iterrows():
                try:
                    post = _data['id']
                except:
                    post = ''
                try:
                    back_off = _data['back_off']
                except:
                    back_off = 0
                try:
                    page_name = _data['pagename']
                except:
                    page_name = ''

                try:
                    _scrapped_date = _data['scrapped_date']
                except:
                    _scrapped_date = scrapped_at

                if auth_flag and graph:
                    print 'Authorised...'
                    try:
                        if str(post).strip():
                            data_likes = self.AuthArch.get_total_likes(
                                graph, post, back_off, data_likes, proxy=proxy,
                                _http=http_proxy)
                    except:
                        time.sleep(1)
                        auth_flag, graph = self.AuthArch.fb_auth(self.url)
                        time.sleep(1)
                        try:
                            if str(post).strip():
                                data_likes = self.AuthArch.get_total_likes(
                                    graph, post, back_off, data_likes,
                                    proxy=proxy, _http=http_proxy)
                        except:
                            data_likes = []

                else:
                    time.sleep(1)
                    auth_flag, graph = self.AuthArch.fb_auth(self.url)
                    time.sleep(1)
                    try:
                        if str(post).strip():
                            data_likes = self.AuthArch.get_total_likes(
                                graph, post, back_off, data_likes, proxy=proxy,
                                _http=http_proxy)
                    except:
                        data_likes = []

                if len(data_likes) > 0:
                    for _like in data_likes:
                        if _like and str(page_name).strip():
                            try:
                                user_id = _like.get('id', 0)
                            except:
                                user_id = ''
                            if str(user_id).strip():
                                new_likes = [page_name, str(post), user_id,
                                             _scrapped_date]
                                all_likes.append(new_likes)

        return all_likes

    def post_to_csv(self, final_data, pages, since):
        """Function to convert posts into CSV file."""
        # Connecting to FB Graph API
        time.sleep(1)
        auth_flag, graph = self.AuthArch.fb_auth(self.url)
        time.sleep(1)
        # Connected to FB Graph API
        scrapped_date = datetime.datetime.strftime(
            datetime.datetime.now(), '%d-%m-%Y %H:%M')

        for page in pages:
            page = pages.get(page, None)
            if page:
                if auth_flag and graph:
                    try:
                        _data = self.AuthArch.get_updated_posts(graph, page,
                                                                since)
                    except:
                        time.sleep(1)
                        auth_flag, graph = self.AuthArch.fb_auth(self.url)
                        time.sleep(1)
                        try:
                            _data = self.AuthArch.get_updated_posts(
                                graph, page, since)
                        except:
                            _data = []
                else:
                    time.sleep(1)
                    auth_flag, graph = self.AuthArch.fb_auth(self.url)
                    time.sleep(1)
                    try:
                        _data = self.AuthArch.get_updated_posts(graph, page,
                                                                since)
                        time.sleep(1)
                    except:
                        _data = []

                time.sleep(1)
                if len(_data) > 0:
                    for _post in _data:
                        if _post:
                            link = _post.get('link')
                            picture = _post.get('picture')
                            hidden = str(_post.get('is_hidden'))
                            expired = str(_post.get('is_expired'))
                            ptype = _post.get('type')
                            status = _post.get('status_type')
                            shares = _post.get('shares')
                            shares = shares.get('count') if shares else 0
                            regex = re.compile('[\n\r\t]')
                            try:
                                message = regex.sub(
                                    ' ', _post.get('message').encode('utf-8'))
                            except:
                                message = _post.get('message')
                            pid = _post.get('id')
                            userid = str(long(_post.get('from').get('id')))
                            username = _post.get('from').get('name')
                            pagename = page
                            # Converting Date time format
                            created_time = FBProcess.date_conv(
                                _post.get('created_time'))

                            updated_time = FBProcess.date_conv(
                                _post.get('updated_time'))

                            comments, com_flag = self.AuthArch.get_summary(
                                graph, pid, 'comments')
                            time.sleep(1)
                            likes = self.AuthArch.get_summary(graph, pid,
                                                              'likes')
                            time.sleep(1)
                            scrap_date = scrapped_date

                            post_data = ['Facebook', pid, pagename, userid,
                                         username, created_time, updated_time,
                                         scrap_date, comments, likes, ptype,
                                         status, hidden, expired, shares,
                                         message, picture, link]

                            final_data.append(post_data)
        return final_data

    def get_comments(self, pages, p_ids, all_com, since, **kwargs):
        """Function to convert comments into CSV file."""
        # ----------------- LOOKING FOR PROXY OPTIONS ---------------------- #
        proxy = False
        http_proxy = False
        if kwargs:
            if kwargs.get('proxy', False) and kwargs.get('HTTP_PROXY'):
                proxy = kwargs.get('proxy', False)
                http_proxy = kwargs.get('HTTP_PROXY')
        # ------------------------------------------------------------------ #
        # Connecting to FB Graph API
        time.sleep(1)
        auth_flag, graph = self.AuthArch.fb_auth(self.url)
        time.sleep(1)
        # Connected to FB Graph API
        scrape_date = datetime.datetime.strftime(
            datetime.datetime.now(), '%d-%m-%Y %H:%M')

        for bname in pages:
            posts = list(p_ids.get(bname, []))
            if len(posts) > 0:
                if auth_flag and graph:
                    try:
                        _data = self.AuthArch.get_total_comments(
                            graph, posts, since, proxy=proxy,
                            http_proxy=http_proxy)
                    except:
                        time.sleep(1)
                        auth_flag, graph = self.AuthArch.fb_auth(self.url)
                        time.sleep(1)
                        try:
                            _data = self.AuthArch.get_total_comments(
                                graph, posts, since, proxy=proxy,
                                http_proxy=http_proxy)
                        except:
                            _data = []
                else:
                    time.sleep(1)
                    auth_flag, graph = self.AuthArch.fb_auth(self.url)
                    time.sleep(1)
                    try:
                        _data = self.AuthArch.get_total_comments(
                            graph, posts, since, proxy=proxy,
                            http_proxy=http_proxy)
                        time.sleep(1)
                    except:
                        _data = []

                time.sleep(1)
                for _comm in _data:
                    if _comm:
                        for com in _comm:
                            user_id = str(long(com.get('from').get('id')))
                            try:
                                user_name = com.get('from').get(
                                    'name').encode('utf-8')
                            except:
                                user_name = com.get('from').get('name')
                            likes = com.get('like_count')
                            can_remove = com.get('can_remove')
                            _time = com.get('created_time')
                            # Converting Date format.
                            _time = FBProcess.date_conv(_time)

                            regex = re.compile('[\n\r\t]')
                            try:
                                _com = regex.sub(
                                    ' ', com.get('message').encode('utf-8'))
                            except:
                                _com = com.get('message')
                            com_id = com.get('id')
                            user_like = com.get('user_like')
                            scrap_date = scrape_date
                            coms = ['Facebook', com_id, bname, user_id,
                                    user_name, user_like, likes, can_remove,
                                    _time, scrap_date, _com]
                            all_com.append(coms)
        return all_com

    def post_stats_cal(self, post_file):
        """Function to calculate latest stats of POSTS."""
        df = pd.read_csv(post_file, encoding='utf-8')

        df['created_time'] = pd.to_datetime(
            df['created_time'], format="%d-%m-%Y %H:%M")
        max_date = df['created_time'].max()
        # ------------ Taking only last Two Months old posts -------------- #
        min_date = max_date - datetime.timedelta(days=59)
        # ----------------------------------------------------------------- #
        _df = df[df['created_time'] >= min_date]
        # ----------------- Dropping Duplicate Values --------------------- #
        _df = _df.sort(['comments', 'likes', 'shares'], ascending=False)
        _df = _df.drop_duplicates(subset=['id'])
        # ----------------------------------------------------------------- #
        df['created_time'] = df['created_time'].apply(
            lambda x: x.strftime("%d-%m-%Y %H:%M"))
        _df['verify'] = 0
        _df['com_track'] = 0
        _df['likes_track'] = 0
        _df['back_off'] = 0
        # -------------------- Getting Updated Time ------------------------ #
        _updatetime = datetime.datetime.now()
        _updatetime = _updatetime.strftime('%d-%m-%Y %H:%M')
        # ------------------------------------------------------------------ #
        # --------------------- FB Authentication -------------------------- #
        time.sleep(1)
        auth_flag, graph = self.AuthArch.fb_auth(self.url)
        time.sleep(1)
        # ------------------------------------------------------------------ #

        _df['created_time'] = _df['created_time'].apply(
            lambda x: x.strftime("%d-%m-%Y %H:%M"))

        for index, _data in _df.iterrows():
            try:
                likes = _data['likes']
            except:
                likes = 0
            try:
                shares = _data['shares']
            except:
                shares = 0
            try:
                comments = _data['comments']
            except:
                comments = 0
            # -------------------- Getting STATS --------------------------- #
            if auth_flag and graph:
                try:
                    _stats = self.AuthArch.get_posts_stats(graph, _data['id'])
                except:
                    time.sleep(1)
                    auth_flag, graph = self.AuthArch.fb_auth(self.url)
                    time.sleep(1)
                    try:
                        _stats = self.AuthArch.get_posts_stats(graph,
                                                               _data['id'])
                    except:
                        _stats = {}
            else:
                time.sleep(1)
                auth_flag, graph = self.AuthArch.fb_auth(self.url)
                time.sleep(1)
                try:
                    _stats = self.AuthArch.get_posts_stats(graph, _data['id'])
                    time.sleep(1)
                except:
                    _stats = {}

            # -------------------------------------------------------------- #
            _shares = _stats.get('shares', 0)
            _comments = _stats.get('comments', 0)
            _likes = _stats.get('likes', 0)
            # ------------------ Comparing Updates ------------------------- #
            if _comments > comments or _likes > likes or _shares > shares:
                print '............ Updating Row ............'
                _df.set_value(index, 'likes', max(_likes, likes))
                _df.set_value(index, 'comments', max(_comments, comments))
                _df.set_value(index, 'shares', max(_shares, shares))
                _df.set_value(index, 'updated_time', _updatetime)
                _df.set_value(index, 'scrapped_date', _updatetime)
                _df.set_value(index, 'verify', 1)

            if _comments > comments:
                print 'Got new Comments..'
                _df.set_value(index, 'com_track', 1)

            if _likes > likes:
                print 'Got new likes..'
                _df.set_value(index, 'likes_track', 1)
                _df.set_value(index, 'back_off', (_likes - likes))
            # --------------------------------------------------------------- #

        _df = _df[_df['verify'] == 1]
        _df = _df.drop(['verify'], axis=1)
        # ---------------- Data To extract new comments -------------------- #
        comdf = _df[_df['com_track'] == 1]
        comdf = comdf.drop(['com_track', 'likes_track', 'back_off'], axis=1)
        # ----------------- Data To extract New Likes ---------------------- #
        likes_df = _df[_df['likes_track'] == 1]
        likes_df = likes_df[likes_df['back_off'] > 0]
        likes_df = likes_df[['pagename', 'id', 'back_off']]
        # ------------------------------------------------------------------ #
        _df = _df.drop(['com_track', 'likes_track', 'back_off'], axis=1)
        # ------------------------------------------------------------------ #

        if len(_df):
            df = df.append(_df)
            try:
                df.to_csv(post_file, index=False, encoding='utf-8')
            except:
                try:
                    df.to_csv(post_file, index=False)
                except:
                    pass
        return comdf, likes_df, _df
