#!/usr/bin/python
# -*- coding: utf-8 -*-

import ConfigParser
import time
import redis
import logging
import calendar
import datetime
import json
import re
import sys

import config
import data
import db
from daemon import Daemon
import mq

class Trends(Daemon):
    """
    Trends main class
    """
    def __init__(self, pid_file, sent=None):
        """
        Constructor
        """
        Daemon.__init__(self, pid_file)
        c = config.Config()
        self.config = c.cfg
        self.log = logging.getLogger('trends')
        self.stats_freq = 3600

    def setup(self):
        self.setup_db()
        self.setup_mq()
        self.update_stats()
    
    def setup_db(self):
        # setup db connections
        self.db = db.Db()
        self.db.setup()
        # update persons list
        self.db.set_persons()
        # get latest persons list
        self.persons = self.db.get_persons()
        # if nextPostId does not exist in db, set it
        key = 'nextPostId'
        if not self.db.exists(key):
            self.db.set(key, 0)

    def setup_mq(self):
        self.mq = mq.MQ()
        self.mq.init_consumer(message_callback)

    def update_stats(self): 
        # if last_update does not exist in db, add it
        key = 'statsLastUpdate'
        if self.db.exists(key):
            self.stats_last_update = int(self.db.get(key))
            diff = int(time.time()) - self.stats_last_update
            if diff >= 0:
                periods = (diff / self.stats_freq) + 1
                self.fill_stats(periods)
        else:
            # we want the stats update to happen at the top of the hour
            t = int(time.time())
            s = time.gmtime(t)
            t -= (s.tm_min * 60 + s.tm_sec) 
            self.stats_last_update = t
            self.fill_stats(1)
            self.db.set('statsFirstUpdate', self.stats_last_update)

    def run(self):
        self.setup()
        self.p_ttime = 0
        self.consumer.wait()
    
    def message_callback(self, message):
        self.p_time = time.time()
        post = data.parse_post(message.body)
        self.process_post(post)
        self.p_ttime += time.time() - self.p_time

    def process_post(self, post):
        """
        Process post received from the Twitter streaming API

        @param post post to process
        """
        # is this a post matching one or more persons?
        post_add = False
        current_time = int(time.time())
        text = data.normalize(post['text']).lower()
        self.first_person = None
        if data.get_text_language(text_stripped) == 'fr':
            for person in self.persons:
                names = data.get_names(person)
                if utils.check_names(names, text, person['words']) == 1:
                    if not post_add:
                        post_add = True
                        # get next post id
                        post_id = self.db.incr('nextPostId')
                    # one more post for this person
                    # if just entered the next hour, we initialize the stats
                    # for all persons
                    diff = current_time - self.stats_last_update
                    if diff >= 0:
                        self.fill_stats((diff / 3600) + 1)
                        # update time spent to process posts
                        self.db.rpush('process_time',
                            int(round((self.p_ttime / 3600) * 100)))
                        self.p_ttime = 0
                    # add post to person's posts list
                    key = 'person:%d:posts:%d' % (person['id'],
                            self.stats_last_update)
                    self.db.rpush(key, post_id)
                    # update stats for this person
                    self.update_person_stats(person)
            if post_add:
                # add post to db
                self.db.set_post(int(post_id),
                    '%s:<$>:%d' % (post['msg'], sv))
                # add post id to current hour
                key = 'posts:%d' % (self.stats_last_update)
                self.db.rpush(key, post_id)
        else:
            logging.debug('found english word in %s', text_stripped)
            
    def update_person_stats(self, person):
        # update stats for this person
        key = 'person:%d:posts_count' % (person['id'])
        v = int(self.db.lindex(key, -1))
        self.db.lset(key, -1, str(v+1))
        if not self.first_person:
            self.first_person = person
        else:
            key = 'person:%d:rel' % (self.first_person['id'])
            v = self.db.lindex(key, -1)
            d = json.loads(v)
            if str(person['id']) in d:
                d[str(person['id'])] += 1
            else:
                d[str(person['id'])] = 1
            self.db.lset(key, -1, json.dumps(d))

    def fill_stats(self, periods):
        for i in range(periods):
            self.stats_last_update += self.stats_freq
            for p in self.persons:
                key = 'person:%d:posts_count' % (p['id'])
                self.db.rpush(key, 0)
                key = 'person:%d:rel' % (p['id'])
                self.db.rpush(key, json.dumps({}))
        key = 'statsLastUpdate'
        self.db.set(key, self.stats_last_update)

if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == 'test':
        trends = Trends('/tmp/trends.pid', test_trends.Sentiment)
    else:
        trends = Trends('/tmp/trends.pid')
    if len(sys.argv) == 2 and sys.argv[1] != 'test':
        if 'start' == sys.argv[1]:
            trends.start()
        elif 'stop' == sys.argv[1]:
            trends.stop()
        elif 'restart' == sys.argv[1]:
            trends.restart()
        else:
            print "Unknown command"
            sys.exit(2)
        sys.exit(0)
    else:
        trends.run()
