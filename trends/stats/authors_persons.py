#!/usr/bin/python
# -*- coding: utf-8 -*-

import ConfigParser
import time
import logging
import calendar
import datetime
import json
import operator
import re
import sys

import numpy as np
import matplotlib.pyplot as plt

import constants
import trends.config as config
import trends.data as data
import trends.db as db
from trends.daemon import Daemon
import utils

class Stats(Daemon):
    """
    Stats main class
    """
    def __init__(self, pid_file, sent=None):
        """
        Constructor
        """
        Daemon.__init__(self, pid_file)
        c = config.Config()
        self.config = c.cfg
        self.log = logging.getLogger('stats')
        # setup db connections
        self.db = db.Db()
        self.db.setup()
        #self.retrieve_data()
        #self.retrieve_data_2()
        self.compute_stats()

    def retrieve_data(self):
        for posts in self.db.iter_posts():
            print posts[0]['post_id']
            for post in posts:
                person_ids = self.db.get_person_ids_from_post_id(
                    post['post_id'])
                author_id = post['author_id']
                key = 'author:%d' % (author_id)
                if not self.db.exists(key):
                    self.db.set(key, '1')
                    self.db.rpush('authors', author_id)
                for person_id in person_ids:
                    self.db.incr('author:%d:person_id:%d' % (
                        author_id, person_id))
    
    def retrieve_data_2(self):
        person_ids = [person[0] for person in constants.persons]
        for i, person_id in enumerate(person_ids):
            for p_id in person_ids:
                key = 'person:%d:p:%d' % (person_id, p_id)
                self.db.delete(key)

        for i, author_id in enumerate(self.db.lrange('authors', 0, -1)):
            print i
            s = []
            for person_id in person_ids:
                r = self.db.get('author:%d:person_id:%d' % (
                        int(author_id), person_id))
                if r:
                    s.append((person_id, int(r)))
            sorted_s = sorted(s, key=operator.itemgetter(1), reverse=True)
            if sorted_s:
                p_id = sorted_s[0][0]
                for person_id, count in sorted_s[1:]:
                    key = 'person:%d:p:%d' % (p_id, person_id)
                    self.db.incr(key)

    def compute_stats(self):
        person_ids = [person[0] for person in constants.persons]
        for person_id in person_ids:
            s = []
            for p_id in person_ids:
                key = 'person:%d:p:%d' % (person_id, p_id)
                v = self.db.get(key)
                if v:
                    s.append((p_id, int(v)))
            sorted_s = sorted(s, key=operator.itemgetter(1), reverse=True)
            tt = sum([e[1] for e in sorted_s])
            name = utils.get_person_name(person_id)
            print name
            for p_id, v in sorted_s[:3]:
                name = utils.get_person_name(p_id)
                print '\t%s - %.1f' % (name, float(v) / tt * 100)
        
if __name__ == "__main__":
  stats = Stats('/tmp/stats.pid')
  if len(sys.argv) == 2:
    if 'start' == sys.argv[1]:
      stats.start()
    elif 'stop' == sys.argv[1]:
      stats.stop()
    elif 'restart' == sys.argv[1]:
      stats.restart()
    else:
      print "Unknown command"
      sys.exit(2)
    sys.exit(0)
  else:
    stats.run()
