import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
from matplotlib.dates import MonthLocator, HourLocator, DateFormatter

import trends.config as config
import trends.data as data
import trends.db as db
from trends.daemon import Daemon

import constants

def get_person_name(person_id):
    for person in constants.persons:
        if person[0] == person_id:
            s = '%s %s' % (person[1], person[2])
            return s.decode('utf-8')

def plot_dates(start, end, vdates, values, person):
    dates = [data.get_fr_datetime_from_timestamp(ts)
        for ts in range(start, end, 24*3600)]
    months = MonthLocator(range(1,13), bymonthday=1, interval=2)
    monthsFmt = DateFormatter("%b '%y")
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.xaxis.set_major_locator(months)
    ax.xaxis.set_major_formatter(monthsFmt)
    ax.set_ylabel('Posts count')
    ax.set_title('Posts count for %s' % (get_person_name(person[0])))
    mv = max(values)
    deltax = 24 * 3600 * 40
    deltay = mv / 9.0
    points = []
    for ts, label, c in vdates:
        #x = data.get_fr_datetime_from_timestamp(ts)
        #ax.axvline(x=x, color='r',
        #    linestyle='--')
        tsd = ts + (24 * 3600 * 1)
        x = data.get_fr_datetime_from_timestamp(tsd)
        add = True
        for px, py in points:
            if (tsd >= px - deltax and tsd <= px + deltax
                and c >= py - deltay and c <= px - deltay):
                add = False
                break
        if add:
            s = data.get_str_from_timestamp(tsd, short=True)
            lb = '%s\n%s' % (s, label)
            ax.text(x, c, lb, color='g', fontsize=8,
                alpha=1.0)
            points.append((tsd, c))
    ax.plot_date(dates, values, 'b-')

