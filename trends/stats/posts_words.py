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
import time

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
        self.compare_persons_words()

    def compute_stats(self):
        start = 1314860400
        end = 1338534000
        for person in self.persons:
            last_id = 0
            words_dict = {}
            process = False
            person_ids = ','.join([str(p['id']) for p in self.persons 
                if p['id'] != person['id']])
            while True:
                print '%d - %d' % (person['id'], last_id)
                # get 1000 post ids from tp_person_post for that person id
                sql = 'select id,post_id from tp_person_post where id >= %s'\
                      ' and person_id = %s order by id limit 1000'
                rows = self.db.mysql_command(
                    'execute', sql, False, last_id, person['id'])
                if not rows:
                    break
                last_id = rows[-1][0]
                post_ids_1 = set([r[1] for r in rows])
                ids = ','.join([str(post_id) for post_id in post_ids_1])
                # get post ids linked to only that person
                sql = 'select post_id from tp_person_post'\
                      ' where post_id in (%s) and person_id in (%s)'\
                      ' order by post_id' % (ids, person_ids)
                rows = self.db.mysql_command(
                    'execute', sql, False)
                post_ids_2 = set([r[0] for r in rows])
                post_ids = post_ids_1.difference(post_ids_2)
                print 'keep %d posts' % (len(post_ids))
                # get posts content from tp_post
                ids = ','.join([str(post_id) for post_id in post_ids])
                sql = 'select post from tp_post where post_id in (%s)' % (
                    ids)
                rows = self.db.mysql_command(
                    'execute', sql, False)
                # update words dict with those posts
                posts_data = [data.parse_post(row[0]) for row in rows]
                posts = [p['text'] for p in posts_data 
                            if p['time'] >= start and p['time'] <= end]
                if process == False and posts:
                    process = True
                elif process == True and not posts:
                    break
                data.update_words_dict(
                    words_dict, posts, self.freq_words, self.persons_words)
            sorted_dict = sorted(
                words_dict.iteritems(), key=operator.itemgetter(1),
                reverse=True)
            with open('words_%d_full.txt' % (person['id']), 'w') as f:
                for v in sorted_dict:
                    f.write('%s\n' % (json.dumps(v)))
    
    def compare_persons_words(self):
        res = []
        for i, person in enumerate(self.persons):
            for person_c in self.persons[i+1:]:
                words = []
                # read person's file entries
                with open('words/words_%d_full.txt' % (person['id']), 'r') as f:
                    for line in f:
                        word = json.loads(line.rstrip('\n'))[0]
                        words.append(word)
                words_c = []
                # read person's file entries
                with open('words/words_%d_full.txt'
                        % (person_c['id']), 'r') as f:
                    for line in f:
                        word = json.loads(line.rstrip('\n'))[0]
                        words_c.append(word)
                same = set(words) & set(words_c)
                res.append((person['name'], person_c['name'], len(same)))
        sorted_res = sorted(res, key=operator.itemgetter(2), reverse=True)
        for e in sorted_res:
            print e

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
