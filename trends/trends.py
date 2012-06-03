#!/usr/bin/python
# -*- coding: utf-8 -*-

import ConfigParser
import time
import redis
import logging
import calendar
import datetime
import json
import re
import sys

import data
import db
from daemon import Daemon
import mq
import utils

class Trends(Daemon):
    """
    Trends main class
    """
    def __init__(self, pid_file, sent=None):
        """
        Constructor
        """
        Daemon.__init__(self, pid_file)
        c = config.Config()
        self.config = c.cfg
        self.log = logging.getLogger('trends')
        self.stats_freq = 3600
        # setup db connections
        self.db = db.Db()
        self.db.setup()
        # update persons list
        self.db.set_persons()
        # get latest persons list
        self.persons = self.db.get_persons()
        # get index where before that, the posts are in mysql 
        self.posts_tid = int(self.db.get('posts_tid'))
        self.update_stats()
        # if nextPostId does not exist in db, set it
        key = 'nextPostId'
        if not self.db.exists(key):
          self.db.set(key, 0)
        self.mq = mq.MQ()
        self.mq.init_consumer(message_callback)

    def update_stats(self): 
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
    self.p_ttime = 0
    self.consumer.wait()
  
  def message_callback(self, message):
    self.p_time = time.time()
    post = data.parse_post(message.body)
    self.process_post(post)
    self.p_ttime += time.time() - self.p_time

  def process_post(self, post):
    """
    Process post received from the Twitter streaming API

    @param status post data
    """
    # is this a post matching one or more persons?
    post_add = False
    current_time = int(time.time())
    text_stripped = utils.strip_accents(post['text']).lower()
    first_person = None
    if utils.get_text_language(text_stripped) == 'fr':
      for person in self.persons:
        names = utils.get_names(person)
        if utils.check_names(names, text_stripped, person['words']) == 1:
          if not post_add:
            post_add = True
            # get next post id
            post_id = self.db.incr('nextPostId')
            if self.process_sentiment:
              # get text's sentiment
              ftext = utils.sentiment_format_post_text(text_stripped,
                self.persons)
              sv = self.sentiment.classify_tweet(ftext)
            else:
              sv = 99
          # one more post for this person
          # if just entered the next hour, we initialize the stats
          # for all persons
          diff = current_time - self.stats_last_update
          if diff >= 0:
            self.fill_stats((diff / 3600) + 1)
            self.db.rpush('process_time',
              int(round((self.p_ttime / 3600) * 100)))
            self.p_ttime = 0
            self.process_sentiment = False
          # add post to person's posts list
          key = 'person:%d:posts:%d' % (person['id'],
              self.stats_last_update)
          self.db.rpush(key, post_id)
          # update stats for this person
          key = 'person:%d:posts_count' % (person['id'])
          v = int(self.db.lindex(key, -1))
          self.db.lset(key, -1, str(v+1))
          if not first_person:
            first_person = person
          else:
            key = 'person:%d:rel' % (first_person['id'])
            v = self.db.lindex(key, -1)
            d = json.loads(v)
            if str(person['id']) in d:
              d[str(person['id'])] += 1
            else:
              d[str(person['id'])] = 1
            self.db.lset(key, -1, json.dumps(d))
          # update sentiment stats and sentiment posts list for this person
          #key = 'person:%d:sentiment' % (person['id'])
          #neg, neu, pos = [int(e) for e in self.db.lindex(key, -1).split(':')]
          #if sv == 8:
          #  neg += 1
          #elif sv == 28:
          #  pos += 1
          #else:
          #  neu += 1
          #self.db.lset(key, -1, '%d:%d:%d' % (neg, neu, pos))
          #key = 'person:%d:%s_posts:%d' % (person['id'],
          #  self.s_map[sv][:3], self.stats_last_update)
          #self.db.rpush(key, post_id)
      if post_add:
        # add post to db
        utils.set_post(int(post_id),
          '%s:<$>:%d' % (post['msg'], sv),
          self.db_posts, self.db_posts_disk, self.posts_tid)
        # add post id to current hour
        key = 'posts:%d' % (self.stats_last_update)
        self.db.rpush(key, post_id)
    else:
      logging.debug('found english word in %s', text_stripped)

  def fill_stats(self, periods):
    for i in range(periods):
      self.stats_last_update += self.stats_freq
      for p in self.persons:
        key = 'person:%d:posts_count' % (p['id'])
        self.db.rpush(key, 0)
        key = 'person:%d:rel' % (p['id'])
        self.db.rpush(key, json.dumps({}))
        #key = 'person:%d:sentiment' % (p['id'])
        #self.db.rpush(key, '0:0:0')
    key = 'statsLastUpdate'
    self.db.set(key, self.stats_last_update)

if __name__ == "__main__":
  if len(sys.argv) == 2 and sys.argv[1] == 'test':
    trends = Trends('/tmp/trends.pid', test_trends.Sentiment)
  else:
    trends = Trends('/tmp/trends.pid')
  if len(sys.argv) == 2 and sys.argv[1] != 'test':
    if 'start' == sys.argv[1]:
      trends.start()
    elif 'stop' == sys.argv[1]:
      trends.stop()
    elif 'restart' == sys.argv[1]:
      trends.restart()
    else:
      print "Unknown command"
      sys.exit(2)
    sys.exit(0)
  else:
    trends.run()
