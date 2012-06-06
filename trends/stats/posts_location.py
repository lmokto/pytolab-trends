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
from mpl_toolkits.basemap import Basemap
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
        count = 0
        while loop:
            # read posts from DB
            rows = self.db.mysql_command('execute', sql, False, last_post_id)
            for row in rows:
                post_id = row[0]
                post = data.parse_post(row[1])
                if post['time'] >= start and post['time'] <= end:
                    count += 1
                    if post['coordinates']:
                        coord = post['coordinates']['coordinates']
                        value = '%f:%f' % (coord[0], coord[1])
                        self.db.rpush('coordinates', value)
                elif post['time'] > end:
                    loop = False
                    break
            last_post_id = post_id
            print last_post_id
        print count

    def compute_stats(self):
        m = Basemap(projection='merc', resolution='c')
        m.drawcoastlines()
        m.drawcountries()
        m.fillcontinents(color = 'coral')
        coordinates = self.db.lrange('coordinates', 0, -1)
        lons = []
        lats = []
        for c in coordinates:
            lon, lat = [float(e) for e in c.split(':')]
            lons.append(lon)
            lats.append(lat)
        x, y = m(lats, lons)
        m.plot(x, y)
        plt.savefig('./map.png')

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
