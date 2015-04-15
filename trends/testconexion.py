#!/usr/bin/env python
# -*- coding: utf-8 -*-

db_disk_posts = MySQLdb.connect(host="localhost", user="monty", passwd="some_pass", db="test_mysql_db", use_unicode=True, charset='utf8')