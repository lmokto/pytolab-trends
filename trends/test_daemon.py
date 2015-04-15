#!/usr/bin/env python
# -*- coding: utf-8 -*-


from daemon import Daemon
import sys

class MyDaemon(Daemon):


	def run(self):
		while True:
			time.sleep(1) 