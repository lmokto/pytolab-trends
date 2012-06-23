#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
import logging
import os
import time
import unittest2 as unittest

from mock import Mock, patch, call

import trends.db as db
import trends.tweets as tweets
import trends.exceptions as exceptions
    
logging.basicConfig(filename='log_test_trends.txt',level=logging.DEBUG)

class Test(unittest.TestCase):

    def setUp(self):
        self.tweets = tweets.Tweets('tweets.pid')
    
    @patch.object(time, 'time')
    @patch.object(logging, 'exception')
    def test_stream_filter(self, time_mock, logging_mock):
        time_mock.side_effect = Exception()
        logging_mock.return_value = None
        self.tweets.persons = [{'name': 'test_namé'}]
        self.tweets.stream = Mock()
        self.tweets.stream.filter.side_effect = Exception()
        self.assertRaises(Exception,
            self.tweets.stream_filter)
        self.tweets.stream.filter.assert_called_once_with(track=['test_name'])


if __name__ == '__main__':
    unittest.main()
