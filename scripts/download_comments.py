#!/usr/bin/env python
# Adapted from https://github.com/egbertbouman/youtube-comment-downloader

from __future__ import print_function

import os
import sys
import time
import json
import requests
import argparse
from datetime import datetime
import numpy as np
import bs4
import pytz
import lxml.html
from lxml.cssselect import CSSSelector

YOUTUBE_COMMENTS_URL = 'https://www.youtube.com/all_comments?v={youtube_id}'
YOUTUBE_COMMENTS_AJAX_URL = 'https://www.youtube.com/comment_ajax'

USER_AGENT = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36'

def find_value(html, key, num_chars=2):
    pos_begin = html.find(key) + len(key) + num_chars
    pos_end = html.find('"', pos_begin)

    return html[pos_begin: pos_end]

def extract_comments(html):
    tree = lxml.html.fromstring(html)
    item_sel = CSSSelector('.comment-item')
    text_sel = CSSSelector('.comment-text-content')
    time_sel = CSSSelector('.time')
    author_sel = CSSSelector('.user-name')
    lvote_sel = CSSSelector('.like-count')
    dvote_sel = CSSSelector('.dislike-count')

    for item in item_sel(tree):
        if (len(dvote_sel(item)) > 0) and (len(lvote_sel(item)) > 0):
            yield {'cid': item.get('data-cid'),
                   'text': text_sel(item)[0].text_content(),
                   'time': time_sel(item)[0].text_content().strip(),
                   'author': author_sel(item)[0].text_content(),
                   'clikes': lvote_sel(item)[0].text_content(),
                   'cdislikes': dvote_sel(item)[0].text_content()}
        elif (len(dvote_sel(item)) == 0) and (len(lvote_sel(item)) == 0):
            yield {'cid': item.get('data-cid'),
                   'text': text_sel(item)[0].text_content(),
                   'time': time_sel(item)[0].text_content().strip(),
                   'author': author_sel(item)[0].text_content(),
                   'clikes': 0,
                   'cdislikes': 0}
        elif (len(lvote_sel(item)) == 0):
            yield {'cid': item.get('data-cid'),
                   'text': text_sel(item)[0].text_content(),
                   'time': time_sel(item)[0].text_content().strip(),
                   'author': author_sel(item)[0].text_content(),
                   'clikes': 0,
                   'cdislikes': dvote_sel(item)[0].text_content()}
        else:
            yield {'cid': item.get('data-cid'),
                   'text': text_sel(item)[0].text_content(),
                   'time': time_sel(item)[0].text_content().strip(),
                   'author': author_sel(item)[0].text_content(),
                   'clikes': lvote_sel(item)[0].text_content(),
                   'cdislikes': 0}

def extract_reply_cids(html):
    tree = lxml.html.fromstring(html)
    sel = CSSSelector('.comment-replies-header > .load-comments')
    return [i.get('data-cid') for i in sel(tree)]


def ajax_request(session, url, params, data, retries=10, sleep_min=60, sleep_max=400):
    for _ in range(retries):
        response = session.post(url, params=params, data=data)
        if response.status_code == 200:
            response_dict = json.loads(response.text)
            return response_dict.get('page_token', None), response_dict['html_content']
        else:
            print('Sleeping...')
            time.sleep(np.random.sample() * np.random.randint(sleep_min, high=sleep_max))

def download_comments(youtube_id, sleep=1):
    session = requests.Session()
    session.headers['User-Agent'] = USER_AGENT

    # Get Youtube page with initial comments
    response = session.get(YOUTUBE_COMMENTS_URL.format(youtube_id=youtube_id))
    html = response.text
    reply_cids = extract_reply_cids(html)

    ret_cids = []
    for comment in extract_comments(html):
        ret_cids.append(comment['cid'])
        yield comment

    page_token = find_value(html, 'data-token')
    session_token = find_value(html, 'XSRF_TOKEN', 4)

    first_iteration = True

    # Get remaining comments (the same as pressing the 'Show more' button)
    while page_token:
        data = {'video_id': youtube_id,
                'session_token': session_token}

        params = {'action_load_comments': 1,
                  'order_by_time': True,
                  'filter': youtube_id}

        if first_iteration:
            params['order_menu'] = True
        else:
            data['page_token'] = page_token

        response = ajax_request(session, YOUTUBE_COMMENTS_AJAX_URL, params, data)
        if not response:
            break

        page_token, html = response

        reply_cids += extract_reply_cids(html)
        for comment in extract_comments(html):
            if comment['cid'] not in ret_cids:
                ret_cids.append(comment['cid'])
                yield comment

        first_iteration = False
        time.sleep(sleep)

    # Get replies (the same as pressing the 'View all X replies' link)
    for cid in reply_cids:
        data = {'comment_id': cid,
                'video_id': youtube_id,
                'can_reply': 1,
                'session_token': session_token}

        params = {'action_load_replies': 1,
                  'order_by_time': True,
                  'filter': youtube_id,
                  'tab': 'inbox'}

        response = ajax_request(session, YOUTUBE_COMMENTS_AJAX_URL, params, data)
        if not response:
            break

        _, html = response

        for comment in extract_comments(html):
            if comment['cid'] not in ret_cids:
                ret_cids.append(comment['cid'])
                yield comment
        time.sleep(sleep)

def get_args():
    parser = argparse.ArgumentParser(add_help=False, description=('Download Youtube comments without using the Youtube API'))
    parser.add_argument('--youtubeids', '-y', help='File containing IDs of Youtube videos for scraping, separated by line', required=True)

    args = parser.parse_args()

    return args

def main():
    args = get_args()

    with open(args.youtubeids) as ytf:
        for youtube_id in ytf:
            print('Downloading data for video:', youtube_id.strip())
            count = 0
            
            with open(youtube_id + '_comments.json', 'w', encoding='utf8') as fp:
                for comment in download_comments(youtube_id.strip()):
                    print(json.dumps(comment, ensure_ascii=False), file=fp)
                    count += 1
                    sys.stdout.write('Downloaded %d comment(s)\r' % count)
                    sys.stdout.flush()

    print('\nDone!')

if __name__ == "__main__":
   main()
