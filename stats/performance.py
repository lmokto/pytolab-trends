#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import sys
import time

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
        self.compute_stats()

    def compute_stats(self):
        n = 10000
        mt = []
        rt = []
        for i in range(10):
            sql = """
                  delete from tp_post_perf
                  """
            self.db.sql_write(sql)
            sql = """
                  insert into tp_post_perf(post_id, post)
                  values(%s, %s)
                  """
            post = 'author_name:<$>:author_id:<$>:id:<$>:text'\
                   ':<$>:0:<$>:null:<$>:time:<$>:sentiment'
            t = time.time()
            for i in range(n):
                self.db.sql_write_no_commit(sql, i, post)
            self.db.sql_commit()
            mt.append(time.time() - t)

            for i in range(n):
                self.db.delete('post:%d' % (i))
            t = time.time()
            for i in range(n):
                self.db.set('post:%d' % (i), post)
            rt.append(time.time() - t)

        print 'mt: %.1f' % (sum(mt) / 10.)
        print 'rt: %.1f' % (sum(rt) / 10.)

        
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
