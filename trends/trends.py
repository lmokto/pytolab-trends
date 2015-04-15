#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ConfigParser
import time
import redis
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
import logging

logger = logging.getLogger('trends')
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

class Trends(Daemon):
    """
    Trends main class
    """
    def __init__(self, pid_file, sent=None):
        #Daemon.__init__(self, pid_file)
        c = config.Config()
        self.config = c.cfg
#        self.log = logging.getLogger('trends')
        self.stats_freq = 3600

    def setup(self):
        """Setup DB connections, message queue consumer and stats."""
        self.setup_db()
        self.setup_mq()
        self.update_stats()
    
    def setup_db(self):
        """Setup DB connections, get initial data from DB."""
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
        """Setup message queue consumer."""
        self.mq = mq.MQ()
        self.mq.init_consumer(self.message_callback)

    def update_stats(self):
        """Fill past persons stats.

        This is in case the script stops for a while and we need to
        catch up.
        """
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
        """Setup and have the consumer wait for the next message
        to consume.
        """
        self.setup()
        self.mq.wait()
    
    def message_callback(self, message):
        """This is called when a new message arrives.

        Load the post attributes values and call the process method on it.
        """
        # if just entered the next hour, we initialize the stats
        # for all persons
        diff = int(time.time()) - self.stats_last_update
        if diff >= 0:
            self.fill_stats((diff / 3600) + 1)
        post = json.loads(message.body)
        self.process_post(post)

    def process_post(self, post):
        """
        Process post received from the message queue.
        """
        # is this a post matching one or more persons?
        post_add = False
        text = data.normalize(post['text']).lower()
        self.first_person = None
        # check post language
        if data.get_text_language(text) == 'es':
            for person in self.persons:
                names = data.get_names(person)
                if data.check_names(names, text, person['words']) == 1:
                    # one more post for this person
                    if not post_add:
                        post_add = True
                        # get next post id
                        post_id = self.db.incr('nextPostId')
                    # add post to person's posts list
                    key = 'person:%d:posts:%d' % (person['id'], self.stats_last_update)
                    self.db.rpush(key, post_id)
                    # update stats for this person
                    self.update_person_stats(person)
            if post_add:
                # add post to db
                self.db.set_post(int(post_id), json.dumps(post))
                # add post id to current hour
                key = 'posts:%d' % (self.stats_last_update)
                self.db.rpush(key, post_id)
        else:
            logger.debug('found english word in %s', text)
            
    def update_person_stats(self, person):
        """Increment person's post count. Update dict of relations with other
        persons. 
        """
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
        """Fill persons stats with default values.

        This is used when we are catching up i.e script stopped for a while.
        """
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
    #
    trends = Trends('/home/delkar/Desktop/pytolabtrends/trends/trends.pid')
    trends.run()
    # if len(sys.argv) == 2:
    #     if 'start' == sys.argv[1]:
    #         trends.start()
    #     elif 'stop' == sys.argv[1]:
    #         trends.stop()
    #     elif 'restart' == sys.argv[1]:
    #         trends.restart()
    #     else:
    #         print "Unknown command"
    #         sys.exit(2)
    #     sys.exit(0)
    # else:
    #     trends.run()
