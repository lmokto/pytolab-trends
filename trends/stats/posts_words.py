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
        self.persons = self.db.set_persons()
        self.persons = self.db.get_persons()
        self.dir_root = self.config.get('trends', 'root')
        self.freq_words = data.get_freq_words(
            '%s/freq_words.txt' % (self.dir_root))
        self.persons_words = data.get_persons_words(self.persons)
        self.compute_stats()

    def compute_stats(self):
        start = 1314860400
        end = 1338534000
        for person in self.persons:
            last_id = 0
            words_dict = {}
            while True:
                print '%d - %d' % (person['id'], last_id)
                # get 1000 post ids from tp_person_post for that person id
                sql = 'select id,post_id from tp_person_post where id > %s'\
                      ' and person_id = %s order by id limit 1000'
                rows = self.db.mysql_command(
                    'execute', sql, False, last_id, person['id'])
                if not rows:
                    break
                last_id = rows[-1][0]
                # get posts content from tp_post
                ids = ','.join([str(row[1]) for row in rows])
                sql = 'select post from tp_post where post_id in (%s)' % (
                    ids)
                rows = self.db.mysql_command(
                    'execute', sql, False)
                # update words dict with those posts
                posts_data = [data.parse_post(row[0]) for row in rows]
                posts = [p['text'] for p in posts_data 
                            if p['time'] >= start and p['time'] <= end]
                data.update_words_dict(
                    words_dict, posts, self.freq_words, self.persons_words)
            sorted_dict = sorted(
                words_dict.iteritems(), key=operator.itemgetter(1),
                reverse=True)
            with open('words_%d.txt' % (person['id']), 'w') as f:
                for v in sorted_dict[:100]:
                    f.write('%s\n' % (json.dumps(v)))

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
