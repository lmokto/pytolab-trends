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
        self.cfg_mysql_user = 'test_mysql_user'
        self.cfg_mysql_password = 'test_mysql_password'
        self.cfg_mysql_db = 'test_mysql_db'
        self.cfg_twitter_userid = 'test_twitter_userid'
        self.cfg_twitter_password = 'test_twitter_password'
        self.cfg_redis_host = 'test_redis_host'
        self.cfg_redis_port = 1
        self.cfg_rabbitmq_host = 'test_rabbitmq_host'
        self.cfg_rabbitmq_userid = 'test_rabbitmq_userid'
        self.cfg_rabbitmq_password = 'test_rabbitmq_password'

    @patch('redis.Redis')
    def test_setup_redis(self, redis_class_mock):
        redis_instance = 'test_redis_instance' 
        redis_class_mock.return_value = redis_instance
        self.db.setup_redis()
        call_args_list = redis_class_mock.call_args_list
        expected = [call(host = self.cfg_redis_host,
                         port = self.cfg_redis_port, db = 0),
                    call(host = self.cfg_redis_host,
                         port = self.cfg_redis_port, db = 1)]
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
        cursor = 'test_cursor'
        mock.cursor.return_value = cursor
        self.db.setup_mysql()
        connect_mock.assert_called_once_with(passwd=self.cfg_mysql_password,
            charset='utf8', db=self.cfg_mysql_db, user=self.cfg_mysql_user,
            use_unicode=True)
        self.assertTrue(mock.cursor.called)
        self.assertIs(self.db.db_disk_posts, mock)
        self.assertIs(self.db.db_cursor, cursor)

    @patch.object(MySQLdb, 'connect')
    def test_setup_mysql_connect_error(self, connect_mock):
        connect_mock.side_effect = MySQLdb.Error
        self.assertRaises(exceptions.DbError, self.db.setup_mysql)

    @patch.object(db.Db, 'setup_mysql')
    def test_setup_mysql_loop(self, setup_mysql_mock):
        self.db.retries = 1
        self.db.setup_mysql_loop()
        self.assertTrue(setup_mysql_mock.called)

    @patch.object(db.Db, 'setup_mysql')
    @patch.object(time, 'sleep')
    def test_setup_mysql_loop_db_error(self, time_sleep_mock, setup_mysql_mock):
        self.db.retries = 2
        self.db.retry_wait = 0.1
        setup_mysql_mock.side_effect = exceptions.DbError() 
        self.assertRaises(exceptions.DbError, self.db.setup_mysql_loop) 
        self.assertEqual(setup_mysql_mock.call_count, 2)
        self.assertEqual(time_sleep_mock.call_count, 2)

    @patch.object(db.Db, 'db_mem')
    def test_redis_command_db_0(self, db_mem_mock):
        db_mem_mock.get.return_value = 'test'
        r = self.db.redis_command(0, 'get', 'test_key')
        db_mem_mock.get.assert_called_once_with(('test_key',))
        self.assertEqual(r, 'test')

    @patch.object(db.Db, 'db_mem_posts')
    def test_redis_command_db_1(self, db_mem_posts_mock):
        db_mem_posts_mock.get.return_value = 'test'
        r = self.db.redis_command(1, 'get', 'test_key')
        db_mem_posts_mock.get.assert_called_once_with(('test_key',))
        self.assertEqual(r, 'test')

    @patch.object(db.Db, 'setup_redis_loop')
    @patch.object(db.Db, 'db_mem')
    def test_redis_command_connection_error(self, db_mem_mock,
            setup_redis_loop_mock):
        mock = Mock()
        mock.side_effect = redis.exceptions.ConnectionError()
        db_mem_mock.get = mock
        setup_redis_loop_mock.side_effect = exceptions.DbError()
        self.assertRaises(exceptions.DbError,
            self.db.redis_command,
            0, 'get', 'test_key')
        db_mem_mock.get.assert_called_once_with(('test_key',))
        setup_redis_loop_mock.assert_called_once()

    @patch.object(db.Db, 'db_mem')
    def test_redis_command_redis_error(self, db_mem_mock):
        self.db.cmd_retries = 2
        self.db.cmd_retry_wait = 0.1
        mock = Mock()
        mock.side_effect = redis.exceptions.RedisError()
        db_mem_mock.get = mock
        self.assertRaises(exceptions.DbError,
            self.db.redis_command,
            0, 'get', 'test_key')
        db_mem_mock.get.assert_called_with(('test_key',))
        self.assertEqual(db_mem_mock.get.call_count, 2)

    @patch.object(db.Db, 'db_mem')
    def test_redis_command_attribute_error(self, db_mem_mock):
        mock = Mock()
        mock.side_effect = AttributeError()
        db_mem_mock.get = mock
        self.assertRaises(exceptions.DbError,
            self.db.redis_command,
            0, 'get', 'test_key')
        db_mem_mock.get.assert_called_once_with(('test_key',))
      
if __name__ == '__main__':
    unittest.main()
