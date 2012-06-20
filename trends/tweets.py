#!/usr/bin/python
# -*- coding: utf-8 -*-

import ConfigParser
import logging
import time
import unicodedata
import json
import sys

import producer
from daemon import Daemon
import utils

import redis
import tweepy

class Tweets(Daemon):
  """
  Tweets main class
  """
  def __init__(self, pid_file):
    """
    Constructor
    """
    logging.basicConfig(filename='log_tweets.txt',level=logging.DEBUG)
    Daemon.__init__(self, pid_file)
    self.persons = []
    self.db = None
    self.config = self.read_config()
    self.setup_db_connection()
    self.persons = utils.get_persons(self.db)
    self.producer = producer.Producer('trends',
        self.config.get('rabbitmq', 'host'),
        self.config.get('rabbitmq', 'userid'),
        self.config.get('rabbitmq', 'password'),
        )
    self.setup_stream_listener()

  def read_config(self):
    """Return config object"""
    config = ConfigParser.ConfigParser()
    config.read('nt.cfg')
    return config

  def setup_db_connection(self):
    """
    Setup connection to Redis
    """
    self.db = redis.Redis(host=self.config.get('redis', 'host'),
        port=self.config.getint('redis', 'port'),
        db=0)
  
  def setup_stream_listener(self):
    """
    Setup Twitter API streaming listenner
    """
    listener = Listener()
    listener.set_tweets(self)
    self.stream = tweepy.Stream(
      self.config.get('twitter', 'userid'),
      self.config.get('twitter', 'password'),
      listener,
      timeout=3600
    )

  def run(self):
    """"""
    # add names to stream filter
    track_list = [self.strip_accents(p['name']) for p in self.persons]
    logging.debug('track_list: %s', track_list)
    while True:
      try:
        self.stream.filter(track=track_list)
      except:
        logging.exception('stream filter')
        time.sleep(10)

  def strip_accents(self, s):
    """
    Strip accents from chars: é -> e
    """
    if isinstance( s, str ):
      s = unicode(s, 'utf-8')
    return ''.join((c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn'))

 
class Listener(tweepy.StreamListener):
  """
  Twitter Streaming API listener
  """
  def on_status(self, status):
    """
    Callback when post is received ok

    @param status post data
    """
    #logging.debug(dir(status))
    if status.author.lang == 'fr':
      logging.debug(status.text)
      message = '%s:<$>:%d:<$>:%d:<$>:%s:<$>:%d:<$>:%s:<$>:%d' % (
          status.author.screen_name, status.author.id,
          status.id, status.text, status.retweeted,
          json.dumps(status.coordinates),
          int(time.time()))
      logging.debug(message)
      self.tweets.producer.publish(message, 'posts')
  
  def on_error(self, status_code):
    """
    Callback when there is an error on the stream

    @param status_code error code
    """
    logging.debug('error: %s', status_code)

  def on_timeout(self):
    """
    Callback when there is a timeout on the stream
    """
    logging.debug('timeout')
    
  def on_limit(self, track):
    """Called when a limitation notice arrives"""
    logging.debug('limit: %s', track)
    return

  def on_delete(self, status_id, user_id):
     """Called when a delete notice arrives for a status"""
     logging.debug('delete: %s - %s', status_id, user_id)
     return

  def set_tweets(self, t):
    """
    Set Tweets class object

    @param t tweets class object
    """
    self.tweets = t

if __name__ == "__main__":
  daemon = Tweets('/tmp/tweets.pid')
  if len(sys.argv) == 2:
    if 'start' == sys.argv[1]:
      daemon.start()
    elif 'stop' == sys.argv[1]:
      daemon.stop()
    elif 'restart' == sys.argv[1]:
      daemon.restart()
    else:
      print "Unknown command"
      sys.exit(2)
    sys.exit(0)
  else:
    print "usage: %s start|stop|restart" % sys.argv[0]
    sys.exit(2)
