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
        self.compute_stats()

    def retrieve_data(self):
        start = 1314860400
        end = 1338534000
        sql = 'select post_id,post from tp_post where post_id > %s'\
              ' order by post_id limit 1000'
        last_post_id = 0
        loop = True
        while loop:
            # read posts from DB
            rows = self.db.mysql_command('execute', sql, False, last_post_id)
            for row in rows:
                post_id = row[0]
                post = data.parse_post(row[1])
                if post['time'] >= start and post['time'] <= end:
                    key = 'author:%d' % (post['author_id'])
                    if not self.db.exists(key):
                        value = '%d:%s' % (
                            post['author_id'], post['author_name'])
                        self.db.rpush('authors', value)
                    self.db.incr(key)
                elif post['time'] > end:
                    loop = False
                    break
            last_post_id = post_id
            print last_post_id

    def compute_stats(self):
        posts_count = []
        authors_data = []
        authors = self.db.lrange('authors', 0, -1)
        for author in authors:
            author_id, author_name = author.split(':')
            c = int(self.db.get('author:%s' % (author_id)))
            posts_count.append(c)
            authors_data.append((author_id, author_name, c))
        sl = sorted(authors_data, key=lambda e: e[2], reverse=True)
        counts = len(posts_count)
        total_counts = sum(posts_count)
        c1 = 0
        c2 = 0
        c3 = 0
        c4 = 0
        c5 = 0
        c6 = 0
        tc1 = 0
        tc2 = 0
        tc3 = 0
        tc4 = 0
        tc5 = 0
        tc6 = 0
        for e in posts_count:
            if e < 200:
                tc1 += e
                c1 += 1
            if e < 300:
                tc2 += e
                c2 += 1
            if e < 500:
                tc3 += e
                c3 += 1
            if e < 1000:
                tc4 += e
                c4 += 1
            if e < 5000:
                tc5 += e
                c5 += 1
            else:
                tc6 += e
                c6 += 1
        print c1
        print c2
        print c3
        print c4
        print c5
        print c6
        print (float(c1) / counts) * 100
        print (float(c2) / counts) * 100
        print (float(c3) / counts) * 100
        print (float(c4) / counts) * 100
        print (float(c5) / counts) * 100
        print (float(c6) / counts) * 100
        print (float(tc1) / total_counts) * 100
        print (float(tc2) / total_counts) * 100
        print (float(tc3) / total_counts) * 100
        print (float(tc4) / total_counts) * 100
        print (float(tc5) / total_counts) * 100
        print (float(tc6) / total_counts) * 100
        for e in sl[:100]:
            print '%s: %d' % (e[1], e[2])
        n, bins, patches = plt.hist(posts_count, bins=100, normed=True,
            log=False, range=(0, 500))
        plt.xlabel('Number of posts')
        plt.ylabel('Frequency')
        plt.title('Posts per author')
        plt.grid(True)
        plt.savefig('./posts_per_author_range_0_500.png')
        n, bins, patches = plt.hist(posts_count, bins=100, normed=True,
            log=True)
        plt.savefig('./posts_per_author_range_log.png')

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
