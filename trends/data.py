#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
import logging
import re
import unicodedata

import exceptions

def parse_post(data):
    try:
        post = {}
        s = data.split(':<$>:')
        post['msg'] = data
        post['author_name'] = s[0]
        post['author_id'] = int(s[1])
        post['id'] = int(s[2])
        post['text'] = s[3]
        post['retweeted'] = int(s[4])
        post['coordinates'] = json.loads(s[5])
        post['time'] = int(s[6])
        post['sentiment'] = int(s[7])
        return post
    except (IndexError, ValueError):
        raise exceptions.DataError()

def normalize(s):
    """
    Strip accents from chars: é -> e
    """
    if isinstance( s, str ):
        s = unicode(s, 'utf-8')
    return ''.join(
        (c for c in unicodedata.normalize('NFD', s) 
            if unicodedata.category(c) != 'Mn'))

def get_persons_words(persons):
    words = []
    for person in persons:
        fn = normalize(person['first_name']).lower().split()
        n = normalize(person['name']).lower().split()
        ni = normalize(person['nickname']).lower().split()
        for e in [fn, n, ni]:
            words.extend(e)
    return [w for w in words if len(w) > 2]

def clean_post(post):
    number_re = re.compile(r'[0-9]+')
    user_re = re.compile(r'@([A-Za-z0-9_ùûüÿàâæçéèêëïîôœ]+)')
    url_re = re.compile(
        r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|'\
        '(?:%[0-9a-fA-F][0-9a-fA-F]))+')
    short_url_re = re.compile(r'[a-zA-Z]+\.[a-zA-Z]+/[a-zA-Z0-9]+')
    punc_re = re.compile(r'[!"#$%&\'()*+,-./:;<=>?\[\\\]^_`{|}~’»«]')
    p = post
    for r in [number_re, user_re, url_re, short_url_re, punc_re]:
        p = re.sub(r, '', p)
    return p

def update_words_dict(words_dict, posts, freq_words, persons_words):
    strs = []
    for post in posts:
        p = clean_post(post.split(':<$>:')[3])
        strs.append(p)
    s = ' '.join(strs)
    words = s.split()
    for w in words:
        wl = w.lower()
        if wl in words_dict:
            words_dict[wl] += 1
        else:
            try:
                wls = normalize(wl)
                if (len(wl) > 2
                        and (wls not in freq_words)
                        and (wls not in persons_words)):
                    words_dict[wl] = 1
            except UnicodeDecodeError, e:
                logging.warning('Cannot process %s', wl)
    return words_dict
 
