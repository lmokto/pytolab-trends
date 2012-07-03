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

import numpy as np
import matplotlib.pyplot as plt

import constants
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
        self.retrieve_data_3()

    def retrieve_data(self):
        start = 1314860400
        end = 1338534000
        count = 0
        last_id = 0
        post_id_start = None
        post_id_end = None
        while True:
            print count
            # get 1000 post ids from tp_person_post for that person id
            sql = 'select id,post_id,post from tp_post where id > %s'\
                  ' order by id limit 1000'
            rows = self.db.mysql_command(
                'execute', sql, False, last_id)
            if not rows:
                break
            last_id = rows[-1][0]
            for row in rows:
                post = data.parse_post(row[2])
                if post['time'] >= start and post['time'] <= end:
                    if not post_id_start:
                        post_id_start = row[1]
                    count += 1
                elif post['time'] >= end:
                    if not post_id_end:
                        post_id_end = row[1]

        print post_id_start
        print post_id_end
        print count
        
    def retrieve_data_2(self):
        counts = []
        post_id_start = 108673
        post_id_end = 8561087
        for person in constants.persons:
            sql = 'select count(*) from tp_person_post'\
                  ' where post_id >= %s and post_id <= %s and person_id = %s'
            row = self.db.mysql_command(
                'execute', sql, False, post_id_start, post_id_end,
                person[0])
            counts.append(row[0][0])

        print counts
        n = len(constants.persons)
        ind = np.arange(n)
        width = 0.5
        fig = plt.figure()
        ax = fig.add_subplot(111)
        rects = ax.bar(ind, counts, width, color='b')
        ax.set_ylabel('Posts Count')
        ax.set_title('Posts Count per candidate')
        ax.set_xticks(ind+width/2)
        ax.set_xticklabels(
            ['%s.%s' % (p[1][0], p[2][0]) for p in constants.persons])
        plt.savefig('./person_posts_count.png')

    def retrieve_data_3(self):
        start = 1314860400
        end = 1338534000
        count = 0
        last_id = 0
        while True:
            # get 1000 post ids from tp_person_post for that person id
            sql = 'select id,post_id,post from tp_post where id > %s'\
                  ' order by id limit 1000'
            rows = self.db.mysql_command(
                'execute', sql, False, last_id)
            if not rows:
                break
            last_id = rows[-1][0]
            for row in rows:
                post = data.parse_post(row[2])
                if post['time'] >= start and post['time'] <= end:
                    if post['retweeted']:
                        count += 1
            print count

        print count
 
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
