#!/usr/bin/env python
# -*- coding: utf-8 -*-

from distutils.core import setup

setup(
    name='NamesTrends',
    version='0.1',
    packages=['tweepy', 'ipython', 'redis', 'amqplib', 'configparser', 'mock', 'unittest2', 'pudb'],
    license='MIT',
    long_description=open('README.txt').read(),
)
