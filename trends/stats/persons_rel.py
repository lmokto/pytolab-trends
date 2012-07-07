#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import sys
import json
import time

import networkx as nx
import pygraphviz as pgv

import constants
import trends.config as config
import trends.data as data
import trends.db as db
from trends.daemon import Daemon

class Stats(Daemon):
    """
    Trends main class
    """
    def __init__(self, pid_file):
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
    
    def compute_stats(self):
        """
        """
        self.graph = nx.Graph()
        persons_post_ids = []
        for person in constants.persons:
            print person[0]
            sql = """
                  select post_id from tp_person_post
                  where person_id = %s order by post_id
                  """
            rows = self.db.sql_read(sql, person[0])
            persons_post_ids.append([row[0] for row in rows])

        for i, p1 in enumerate(constants.persons):
            for j, p2 in enumerate(constants.persons[i+1:]):
                e1 = data.normalize(p1[2])
                e2 = data.normalize(p2[2])
                d = set(persons_post_ids[i]).intersection(
                    set(persons_post_ids[j+i+1]))
                c = len(d)
                if (e1, e2) in self.graph.edges():
                    self.graph.edge[e1][e2]['w'] += c
                elif (e2, e1) in self.graph.edges():
                    self.graph.edge[e2][e1]['w'] += c
                else:
                    self.graph.add_edge(e1, e2, w=c)

        # keep 20 highest weight edges
        edges = sorted(self.graph.edges(data=True),
            key=lambda (u,v,d): d['w'],
            reverse=True)[:20]
        self.graph.clear()
        self.graph.add_edges_from(edges)
        # get highest weight
        hw = max([d['w'] for u,v,d in edges])
        # set edges colors based on weight
        colors = ('#00CCFF', '#0099FF', '#0066FF', '#0033FF', '#0000FF')
        for u,v,d in edges:
            idx = int(round(d['w'] * 4. / hw))
            self.graph.edge[u][v]['color'] = colors[idx]
            self.graph.edge[u][v]['penwidth'] = idx + 1

        nx.write_dot(self.graph, 'stats.dot')
        pyg = pgv.AGraph('stats.dot')
        pyg.graph_attr.update(size='7,7')
        pyg.layout(prog='dot')
        pyg.draw('persons_rel.png')

if __name__ == "__main__":
    c = Stats('/tmp/stats.pid')
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            c.start()
        elif 'stop' == sys.argv[1]:
            c.stop()
        elif 'restart' == sys.argv[1]:
            c.restart()
        else:
            print "Unknown command"
            sys.exit(2)
        sys.exit(0)
    else:
        c.run()

