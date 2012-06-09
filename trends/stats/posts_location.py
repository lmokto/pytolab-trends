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
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import redis

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
        self.retrieve_data_old()
        #self.retrieve_data()
        self.compute_stats()

    def retrieve_data_old(self):
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

    def retrieve_data(self):
        start = 1314860400
        end = 1338534000
        for person in constants.persons:
            last_id = 0
            key = 'coordinates:%d' % (person[0])
            while True:
                # get 1000 post ids from tp_person_post for that person id
                sql = 'select id,post_id from tp_person_post where id > %s'\
                      ' and person_id = %s order by id limit 1000'
                rows = self.db.mysql_command(
                    'execute', sql, False, last_id, person[0])
                if not rows:
                    break
                last_id = rows[-1][0]
                # get posts content from tp_post
                ids = ','.join([str(row[1]) for row in rows])
                sql = 'select post from tp_post where post_id in (%s)' % (
                    ids)
                rows = self.db.mysql_command(
                    'execute', sql, False)
                # parse posts and store coordinates if exists
                for row in rows:
                    post = data.parse_post(row[0])
                    if post['time'] >= start and post['time'] <= end:
                        if post['coordinates']:
                            coord = post['coordinates']['coordinates']
                            value = '%f:%f' % (coord[0], coord[1])
                            self.db.rpush(key, value)

    def init_map(self, x1, x2, y1, y2):
        m = Basemap(resolution='i', projection='merc', llcrnrlat=y1,
            urcrnrlat=y2, llcrnrlon=x1, urcrnrlon=x2, lat_ts=(x1+x2)/2)
        m.drawcoastlines()
        m.drawcountries()
        m.fillcontinents(color = 'coral')
        return m
        
    def compute_stats(self):
        self.generate_maps('full', 'coordinates')
        for person in constants.persons:
            self.generate_maps(str(person[0]), 'coordinates:%d' % (person[0]))
        #self.generate_maps(constants.persons[9])
         
    def generate_maps(self, name, key):
        coordinates = self.db.lrange(key, 0, -1)
        lons = []
        lats = []
        for c in coordinates:
            lon, lat = [float(e) for e in c.split(':')]
            lons.append(lon)
            lats.append(lat)
        
        x1, x2, y1, y2 = -165., 165., -75., 75.
        m  = self.init_map(x1, x2, y1, y2)
        x, y = m(lons, lats)
        m.plot(x, y, 'bo')
        plt.savefig('./world_map_%s.png' % (name))
        plt.clf()

        x1, x2, y1, y2 = -20., 40., 32., 64.
        m  = self.init_map(x1, x2, y1, y2)
        x, y = m(lons, lats)
        m.plot(x, y, 'bo')
        plt.savefig('./europe_map_%s.png' % (name))
        plt.clf()

        x1, x2, y1, y2 = -5., 9., 42., 52.
        m  = self.init_map(x1, x2, y1, y2)
        x, y = m(lons, lats)
        m.plot(x, y, 'bo')
        plt.savefig('./france_map_%s.png' % (name))
        plt.clf()
        
        x1, x2, y1, y2 = 2., 7., 49., 52.
        m  = self.init_map(x1, x2, y1, y2)
        x, y = m(lons, lats)
        m.plot(x, y, 'bo')
        plt.savefig('./belgium_map_%s.png' % (name))
        plt.clf()


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
