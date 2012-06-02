import logging
import time
import unittest2 as unittest

import redis
from mock import patch, call, Mock
import MySQLdb

import trends.db as db
import trends.exceptions as exceptions
    
logging.basicConfig(filename='log_test_db.txt',level=logging.DEBUG)

class DbTest(unittest.TestCase):

    def setUp(self):
        self.db = db.Db()

    @patch('redis.Redis')
    def test_setup_redis(self, redis_class_mock):
        redis_instance = None 
        redis_class_mock.return_value = redis_instance
        self.db.setup_redis()
        call_args_list = redis_class_mock.call_args_list
        expected = [call(host = 'localhost', port = 6379, db = 0),
                    call(host = 'localhost', port = 6379, db = 1)]
        self.assertListEqual(call_args_list, expected)
        self.assertEqual(call_args_list, expected)
        self.assertIs(self.db.db_mem, redis_instance)
        self.assertIs(self.db.db_mem_posts, redis_instance)

    @patch('redis.Redis')
    def test_setup_redis_authentication_error(self, redis_class_mock):
        redis_class_mock.side_effect = redis.exceptions.AuthenticationError() 
        self.assertRaises(exceptions.DbError, self.db.setup_redis) 

    @patch('redis.Redis')
    def test_setup_redis_connection_error(self, redis_class_mock):
        redis_class_mock.side_effect = redis.exceptions.ConnectionError() 
        self.assertRaises(exceptions.DbError, self.db.setup_redis) 

    @patch.object(db.Db, 'setup_redis')
    def test_setup_redis_loop(self, setup_redis_mock):
        self.db.retries = 1
        self.db.setup_redis_loop()
        self.assertTrue(setup_redis_mock.called)

    @patch.object(db.Db, 'setup_redis')
    @patch.object(time, 'sleep')
    def test_setup_redis_loop_db_error(self, time_sleep_mock, setup_redis_mock):
        self.db.retries = 2
        self.db.retry_wait = 0.1
        setup_redis_mock.side_effect = exceptions.DbError() 
        self.assertRaises(exceptions.DbError, self.db.setup_redis_loop) 
        self.assertEqual(setup_redis_mock.call_count, 2)
        self.assertEqual(time_sleep_mock.call_count, 2)
    
    @patch.object(MySQLdb, 'connect')
    def test_setup_mysql(self, connect_mock):
        mock = Mock()
        connect_mock.return_value = mock
        self.db.setup_mysql()
        print connect_mock.call_args_list


if __name__ == '__main__':
    unittest.main()
