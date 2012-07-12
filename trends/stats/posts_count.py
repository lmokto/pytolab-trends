#!/usr/bin/python
# -*- coding: utf-8 -*-

import calendar
import ConfigParser
import time
import logging
import calendar
import collections
import datetime
import json
import re
import sys
import heapq

import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
from matplotlib.dates import MonthLocator, HourLocator, DateFormatter

import trends.config as config
import trends.data as data
import trends.db as db
from trends.daemon import Daemon

import constants

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
        self.retrieve_data_2()
        self.retrieve_data_3()
        #self.compute_stats()

    def retrieve_data(self):
        start = 1314860400
        end = 1338534000
        idx_start = 412
        idx_end = idx_start + 6576
        n = 274
        dates = [data.get_fr_datetime_from_timestamp(ts)
            for ts in range(start, end, 24*3600)]
        # every 3rd month
        months = MonthLocator(range(1,13), bymonthday=1, interval=2)
        monthsFmt = DateFormatter("%b '%y")
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.xaxis.set_major_locator(months)
        ax.xaxis.set_major_formatter(monthsFmt)
        ax.set_ylabel('Posts count')
        ax.set_title('Posts count per candidate')
        ds = []
        ds.append(datetime.datetime(2011, 10, 9, 0, 0, 0, 0))
        ds.append(datetime.datetime(2011, 10, 16, 0, 0, 0, 0))
        ds.append(datetime.datetime(2012, 4, 22, 0, 0, 0, 0))
        ds.append(datetime.datetime(2012, 5, 6, 0, 0, 0, 0))
        for d in ds:
            ts = time.mktime(d.timetuple())
            ax.axvline(x=data.get_fr_datetime_from_timestamp(ts), color='r')
        for person in constants.persons:
            counts = []
            offset = 0
            key = 'person:%d:posts_count' % (person[0])
            for i in range(n):
                s = sum([int(e) for e in self.db.lrange(
                    key, idx_start+offset, idx_start+offset+23)])
                offset += 24
                counts.append(s)
            ax.plot_date(dates, counts, 'b-')
            
        plt.savefig('./posts_count_full.png')

    def retrieve_data_2(self):
        d = datetime.datetime(2012, 4, 22, 8, 0, 0, 0)
        start = int(calendar.timegm(d.utctimetuple()))
        d = datetime.datetime(2012, 4, 22, 20, 0, 0, 0)
        end = int(calendar.timegm(d.utctimetuple()))
        s = 1313373600
        idx_start = (start - s) / 3600
        n = 12
        idx_end = idx_start + n
        dates = [data.get_fr_datetime_from_timestamp(ts)
            for ts in range(start, end, 3600)]
        # every 2 hours
        hours = HourLocator(range(1,25), interval=1)
        hoursFmt = DateFormatter("%H")
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.xaxis.set_major_locator(hours)
        ax.xaxis.set_major_formatter(hoursFmt)
        ax.set_ylabel('Posts count')
        ax.set_title('Posts count per candidate')
        ds = []
        ds.append(datetime.datetime(2012, 4, 22, 18, 0, 0, 0))
        for d in ds:
            ts = time.mktime(d.timetuple())
            ax.axvline(x=data.get_fr_datetime_from_timestamp(ts), color='r')
        colors = {2: 'r', 10: 'm', 16: 'y', 17: 'c', 20: 'g'}
        labels = {2: 'F.B', 10: 'F.H', 16: 'M.L', 17: 'J.L', 20: 'N.S'}
        plots = []
        for person in constants.persons:
            if person[0] in (2, 10, 16, 17, 20):
                counts = []
                offset = 0
                key = 'person:%d:posts_count' % (person[0])
                for i in range(n):
                    s = sum([int(e) for e in self.db.lrange(
                        key, idx_start+offset, idx_start+offset+1)])
                    offset += 1
                    counts.append(s)
                ax.plot_date(dates, counts, '%s-' % (colors[person[0]]),
                    label=labels[person[0]])
            
        ax.legend(loc='upper left')
        plt.savefig('./posts_count_first_round.png')

    def retrieve_data_3(self):
        d = datetime.datetime(2012, 5, 6, 8, 0, 0, 0)
        start = int(calendar.timegm(d.utctimetuple()))
        d = datetime.datetime(2012, 5, 6, 20, 0, 0, 0)
        end = int(calendar.timegm(d.utctimetuple()))
        s = 1313373600
        idx_start = (start - s) / 3600
        n = 12
        idx_end = idx_start + n
        dates = [data.get_fr_datetime_from_timestamp(ts)
            for ts in range(start, end, 3600)]
        # every 2 hours
        hours = HourLocator(range(1,25), interval=1)
        hoursFmt = DateFormatter("%H")
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.xaxis.set_major_locator(hours)
        ax.xaxis.set_major_formatter(hoursFmt)
        ax.set_ylabel('Posts count')
        ax.set_title('Posts count per candidate')
        ds = []
        ds.append(datetime.datetime(2012, 5, 6, 18, 0, 0, 0))
        for d in ds:
            ts = time.mktime(d.timetuple())
            ax.axvline(x=data.get_fr_datetime_from_timestamp(ts), color='r')
        colors = {2: 'r', 10: 'm', 16: 'y', 17: 'c', 20: 'g'}
        labels = {2: 'F.B', 10: 'F.H', 16: 'M.L', 17: 'J.L', 20: 'N.S'}
        plots = []
        for person in constants.persons:
            if person[0] in (10, 20):
                counts = []
                offset = 0
                key = 'person:%d:posts_count' % (person[0])
                for i in range(n):
                    s = sum([int(e) for e in self.db.lrange(
                        key, idx_start+offset, idx_start+offset+1)])
                    offset += 1
                    counts.append(s)
                ax.plot_date(dates, counts, '%s-' % (colors[person[0]]),
                    label=labels[person[0]])
            
        ax.legend(loc='upper left')
        plt.savefig('./posts_count_second_round.png')


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
