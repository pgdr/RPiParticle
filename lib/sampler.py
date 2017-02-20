import time
from datetime import datetime as dt
from ts import TS
import sqlite3 as sql
import sys

class Sampler(object):

    def __init__(self, reader, sample_time , sql_path, sleep_time = 0.10, accuracy = None):
        """
        The sqlite db has a table called 'samples' with schema
        id, value, sensor, timestamp, uploaded
        """
        self.reader = reader
        self.sample_time = sample_time
        self.sleep_time = sleep_time
        self.accuracy = accuracy
        self.sql_path = sql_path


    def _insert(self, val, sensor):
        """Returns INSERT query"""
        return 'INSERT INTO samples (value, sensor, timestamp) VALUES (%f, %s, %s)' % (val, sensor, dt.now())

    def _write_ts_to_sql(self, data):
        ts_pm10, ts_pm25 = data
        q10 = self.__insert(ts_pm10.median(), 'PM10')
        q25 = self.__insert(ts_pm25.median(), 'PM25')
        try:
            con = sqlite3.connect(self.sql_path)
            con.execute(q10)
            con.execute(q25)
        except Exception as e:
            sys.stderr.write('Error on persisting data: %s.' % e)
        finally:
            con.close()


    def collect(self):
        """Reads values from the sensor and writes it to its sql database.
        """
        # reader/sds011 returns (PM10, PM25)
        data = (TS(accuracy=self.accuracy), TS(accuracy=self.accuracy))
        start = dt.now( )
        while True:
            pm10,pm25 = self.reader.read( )
            data[0].append( pm10 )
            data[1].append( pm25 )

            dt_now = dt.now( ) - start
            if dt_now.total_seconds() >= self.sample_time:
                break

            time.sleep( self.sleep_time )

        self._write_ts_to_sql(data)
