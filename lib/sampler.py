import time
from datetime import datetime as dt
from ts import TS
from friskby_dao import FriskbyDao

class Sampler(object):
    """This class is initialized with a reader (a sensor of type SDS011), a path to
    an sqlite database file and sample time.  The collect method collects data
    for `sample_time` amount of time, and then asks the FriskbyDao to persist
    this data.

    """

    def __init__(self, reader, sql_path, sample_time, sleep_time=0.10, accuracy=None):
        """
        The sqlite db has a table called 'samples' with schema
        id, value, sensor, timestamp, uploaded
        """
        self.reader = reader
        self.sample_time = sample_time
        self.sleep_time = sleep_time
        self.accuracy = accuracy
        self.dao = FriskbyDao(sql_path)

    def collect(self):
        """Reads values from the sensor and writes it to its sql database.
        """
        # reader/sds011 returns (PM10, PM25)
        data = (TS(accuracy=self.accuracy), TS(accuracy=self.accuracy))
        start = dt.now()
        while True:
            pm10, pm25 = self.reader.read()
            data[0].append(pm10)
            data[1].append(pm25)

            dt_now = dt.now() - start
            if dt_now.total_seconds() >= self.sample_time:
                break
            time.sleep(self.sleep_time)
        self.dao.persist_ts(data)
