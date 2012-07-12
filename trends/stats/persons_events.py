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
import heapq

import numpy as np
import matplotlib.mlab as mlab
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
        self.persons = self.db.get_persons()
        self.dir_root = self.config.get('trends', 'root')
        self.freq_words = data.get_freq_words(
            '%s/freq_words.txt' % (self.dir_root))
        self.persons_words = data.get_persons_words(self.persons)
        self.retrieve_data()
        #self.compute_stats()

    def retrieve_data(self):
        start = 1314860400
        end = 1338534000
        persons_stats = []
        for person in constants.persons:
            print '%s %s' % (person[1], person[2])
            s = []
            for time_slot in range(start, end+3600, 24*3600):
                sql = """
                      select count(*) from tp_person_post
                      where person_id=%s and time_slot >= %s
                      and time_slot < %s
                      """
                rows = self.db.sql_read(sql, person[0], time_slot,
                    time_slot + 24*3600)
                s.append((time_slot, rows[0][0]))
            s = sorted(s, key=operator.itemgetter(1), reverse=True)
            persons_stats.append(s[:100])
        
        for i, person in enumerate(constants.persons):
            print '%s %s' % (person[1], person[2])
            vdates = []
            for t, c in persons_stats[i]:
                print data.get_str_from_timestamp(t)
                words_dict = {}
                last_id = 0
                while True:
                    sql = """
                          select post_id from tp_person_post
                          where post_id > %s and person_id=%s
                          and time_slot >= %s and time_slot < %s
                          order by post_id limit 1000
                          """
                    rows = self.db.sql_read(sql, last_id, person[0], t,
                        t + 24*3600)
                    if not rows:
                        break
                    last_id = rows[-1][0]
                    ids = ','.join([str(row[0]) for row in rows])
                    sql = 'select post from tp_post where post_id in (%s)' % (
                        ids)
                    rows = self.db.sql_read(sql)
                    posts = [data.clean_post(row[0]) for row in rows]
                    data.update_words_dict(
                        words_dict, posts, self.freq_words, self.persons_words)
                sd = sorted(
                    words_dict.iteritems(), key=operator.itemgetter(1),
                    reverse=True)
                most = []
                for w, _ in sd:
                    dup = False
                    for e in most:
                        wn = data.normalize(w)
                        en = data.normalize(e)
                        if en in wn or wn in en:
                            dup = True
                            break
                    if not dup:
                        most.append(w)
                    if len(most) == 3:
                        break
                label = u'\n'.join(most)
                vdates.append((t, label, c))
            print vdates
            # plot chart
            values = []
            key = 'person:%d:posts_count' % (person[0])
            idx_start = 413
            n = 274
            offset = 0
            for i in range(n):
                s = sum([int(e) for e in self.db.lrange(
                    key, idx_start+offset, idx_start+offset+23)])
                offset += 24
                values.append(s)
            utils.plot_dates(start, end, vdates, values, person)
            plt.savefig('./person_events_%d.png' % (person[0]))

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
