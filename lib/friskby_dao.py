from sys import stderr, argv
from os.path import abspath, isfile
from datetime import datetime as dt
import sqlite3

class FriskbyDao(object):

    # CREATE TABLE samples (`id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,


    def __init__(self, sql_path):
        """The sqlite db has a table called 'samples' with schema
        id, value, sensor, timestamp, uploaded
        """
        self._sql_path = abspath(sql_path)
        print('FriskbyDao(%s)' % self._sql_path)
        self.__init_sql()

    def __init_sql(self):
        if not isfile(self._sql_path):
            print('No existing database, creating ... ')
            _id    = '`id` INTEGER PRIMARY KEY'
            _val   = '`value` FLOAT NOT NULL'
            _sen   = '`sensor` TEXT NOT NULL'
            _date  = '`timestamp` TEXT NOT NULL'
            _upl   = '`uploaded` BOOL DEFAULT 0'
            schema = 'CREATE TABLE samples (%s, %s, %s, %s, %s);' % (_id, _val, _sen, _date, _upl)
            print('Created:   %s' % schema)
            conn   = sqlite3.connect(self._sql_path)
            conn.execute(schema)
            conn.close()

    def get_non_uploaded(self, limit=100):
        q = 'SELECT * FROM samples WHERE NOT `uploaded` LIMIT %d;' # extract 100 to config?
        try:
            conn = sqlite3.connect(self._sql_path)
            result = conn.execute(q % limit)
            data = result.fetchall()
            conn.close()
            return data
        except Exception as e:
            stderr.write('Error on reading data: %s.\n' % e)

    def _insert(self, val, sensor):
        """Returns INSERT query"""
        return "INSERT INTO samples (id, value, sensor, timestamp) VALUES (NULL, %f, '%s', '%s');" % (val, sensor, dt.utcnow())

    def persist_ts(self, data):
        ts_pm10, ts_pm25 = data
        q10 = self._insert(ts_pm10.median(), 'PM10')
        q25 = self._insert(ts_pm25.median(), 'PM25')
        try:
            conn = sqlite3.connect(self._sql_path)
            conn.execute(q10)
            conn.execute(q25)
            conn.commit()
            conn.close()
        except Exception as e:
            stderr.write('Error on persisting data: %s.\n' % e)
            stderr.write('                     q10: %s.\n' % q10)
            stderr.write('                     q25: %s.\n' % q25)


    def mark_uploaded(self, data):
        q = 'UPDATE samples SET uploaded=1 WHERE id=%s'
        try:
            conn = sqlite3.connect(self._sql_path)
            conn.execute('begin')
            for d in data:
                # id, value, sensor, timestamp, uploaded
                id_ = d[0]
                conn.execute(q%id_)
            conn.commit()
            conn.close()
        except Exception as e:
            stderr.write('Error on setting UPLOADED data! %s.\n' % e)



if __name__ == '__main__':
    if len(argv) > 1:
        f = FriskbyDao(argv[1])
    else:
        print('Needs path to db')
