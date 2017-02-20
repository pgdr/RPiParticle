from tempfile import NamedTemporaryFile as temp
from datetime import datetime as dt
from unittest import TestCase
import os.path
from ts import TS
from friskby_dao import FriskbyDao
from random import random as rnd
import sqlite3

def rand():
    return round(rnd()*100, 2)

def gen_rand_ts():
    t = TS()
    for i in range(3):
        t.append(rand())
    return t

class DaoTest(TestCase):

    def setUp(self):
        tmpf = temp(delete=False)
        self.fname = tmpf.name + '_db.sql'
        tmpf.close()
        self.dao = FriskbyDao(self.fname)

    def test_created(self):
        q = "SELECT name FROM sqlite_master WHERE type='table';"
        conn = sqlite3.connect(self.fname)
        c = conn.execute(q)
        result = c.fetchall()
        self.assertEqual(1, len(result))
        self.assertEqual('samples', result[0][0])

    def test_persist(self):
        num_data = 13
        for i in range(num_data):
            t10 = gen_rand_ts()
            t25 = gen_rand_ts()
            self.dao.persist_ts((t10,t25))
        data = self.dao.get_non_uploaded(limit=30)
        self.assertEqual(2*num_data, len(data))

    def test_mark_uploaded(self):
        num_data = 17
        for i in range(num_data):
            t10 = gen_rand_ts()
            t25 = gen_rand_ts()
            self.dao.persist_ts((t10,t25))
        print(repr(self.dao))
        data = self.dao.get_non_uploaded(limit=30)
        self.assertEqual(30, len(data))
        self.dao.mark_uploaded(data)
        data = self.dao.get_non_uploaded(limit=30)
        self.assertEqual(4, len(data)) # total 34, marked 30
        print(repr(self.dao))
