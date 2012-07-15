#!/usr/bin/python
# -*- coding: utf-8 -*-

import ConfigParser
import time
import logging
import calendar
import datetime
import json
import re
import sys
import heapq

import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import redis

import trends.config as config
import trends.data as data
import trends.db as db
from trends.daemon import Daemon

import constants

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
        #self.compute_stats()
        self.draw_figure()

    def retrieve_data(self):
        for posts in self.db.iter_posts():
            print posts[0]['post_id']
            for p in posts:
                author_id = p['author_id']
                key = 'author:%d' % (author_id)
                if not self.db.exists(key):
                    self.db.set(key, '1')
                    self.db.rpush('authors', author_id)
                person_ids = self.db.get_person_ids_from_post_id(p['post_id'])
                for p_id in person_ids:
                    self.db.incr('author:%d:p:%d' % (author_id, p_id))

    def compute_stats(self):
        for person in constants.persons:
            self.db.delete('person:%d' % (person[0]))
        for i, author_id in enumerate(self.db.lrange('authors', 0, -1)):
            print author_id
            person_counts = []
            for person in constants.persons:
                v = self.db.get('author:%d:p:%d' % (int(author_id), person[0]))
                if v:
                    v = int(v)
                else:
                    v = 0
                person_counts.append(v)
            s = sum(person_counts)
            if s:
                for i, c in enumerate(person_counts):
                    if float(c) / s > 0.8:
                        self.db.incr('person:%d' % (constants.persons[i][0]))

    def draw_figure(self):
        n = len(constants.persons)
        ind = np.arange(n)
        width = 0.35
        fig = plt.figure()
        ax = fig.add_subplot(111)
        values = [int(self.db.get('person:%d' % p[0]))
            for p in constants.persons]
        rects = ax.bar(ind, values, width, color='b')
        ax.set_ylabel('Authors count')
        ax.set_title('Candidate fans count')
        ax.set_xticks(ind+width/2)
        ax.set_xticklabels(
            ['%s.%s' % (p[1][0], p[2][0]) for p in constants.persons])
        plt.savefig('./authors_persons_fans.png')

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
