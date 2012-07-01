#!/usr/bin/python
# -*- coding: utf-8 -*-

import ConfigParser
import time
import logging
import calendar
import collections
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
        post_id_start = 108673
        post_id_end = 8561087
        sql = 'select post_id,post from tp_post where post_id > %s'\
              ' and post_id >= %s and post_id <= %s'\
              ' order by post_id limit 1000'
        last_post_id = 0
        hours = collections.defaultdict(int)
        while True:
            # read posts from DB
            rows = self.db.mysql_command('execute', sql, False, 
                last_post_id, post_id_start, post_id_end)
            if not rows:
                break
            for row in rows:
                post_id = row[0]
                post = data.parse_post(row[1])
                #if post['author_id'] == 500777999:
                d = data.get_fr_datetime_from_timestamp(post['time'])
                self.db.incr('h:%d:%d' % (post['author_id'], d.hour))
                hours[d.hour] += 1
            last_post_id = post_id
            print last_post_id

        print hours.items()
        n = 24
        ind = np.arange(n)
        width = 0.35
        fig = plt.figure()
        ax = fig.add_subplot(111)
        rects = ax.bar(ind, hours.values(), width, color='b')
        ax.set_ylabel('Posts count')
        ax.set_title('Posts count per hour')
        ax.set_xticks(ind+width/2)
        ax.set_xticklabels([str(h) for h in hours.keys()])
        plt.savefig('./posts_count_hours.png')

    def compute_stats(self):
        robots = 0
        posts_count = 0
        for v in self.db.lrange('authors', 0, -1):
            author_id, author_name = v.split(':')
            # check if each hour has the same number of posts
            # +- 20%
            hours = []
            for i in range(24):
                c = self.db.get('h:%s:%d' % (author_id, i))
                if c:
                    hours.append(int(c))
                else:
                    hours.append(0)
            s = sum(hours)
            avg = (float)(s) / len(hours)
            mn = avg * 0.6
            mx = avg * 1.4
            if all(e >= mn and e <= mx for e in hours):
                print author_name
                print hours
                robots += 1
                posts_count += int(self.db.get('author:%s' % (author_id)))
        print robots
        print posts_count


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
