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
        self.stats()

    def stats(self):
        start = 1315964051
        end = 1337564051
        sql = 'select post_id,post from tp_post where post_id > %s'\
              ' order by post_id limit 1000'
        last_post_id = 0
        loop = True
        authors = 0
        while loop:
            # read posts from DB
            rows = self.db.mysql_command('execute', sql, False, last_post_id)
            for row in rows:
                post_id = row[0]
                post = data.parse_post(row[1])
                if post['time'] >= start and post['time'] <= end:
                    key = 'author:%d', post['author_id']
                    if not self.db.exists(key):
                        authors += 1
                        value = '%d:%s' % (
                            post['author_id'], post['author_name'])
                        self.db.rpush('authors', value)
                    self.db.incr('author:%d' % post['author_id'])
                elif post['time'] > end:
                    loop = False
                    break
            last_post_id = post_id
            print last_post_id

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
