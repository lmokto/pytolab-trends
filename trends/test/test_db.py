import unittest

import namestrends.db

class DbTest(unittest.TestCase):

    def setUp(self):
        self.db = namestrends.db.Db()

    def test(self):
        print self.db

if __name__ == '__main__':
    unittest.main()
