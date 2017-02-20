import os.path
from datetime import datetime as dt
import requests
import json
import sys
import sqlite3
from requests.exceptions import ConnectionError


class FriskbySubmitter(object):

    def __init__(self , device_config , sql_path):
        self.device_config = device_config
        self.sql_path = sql_path

    def _upload(self, rows):
        # id, value, sensor, timestamp, uploaded
        data = {}
        for row in rows:
            id_, value, sensor, timestamp, _ = row
            if sensor not in data:
                data[sensor] = []
            data.append((id_, value, sensor, timestamp))
        data = {"sensorid"   : self.sensor_id,
                "value_list" : self.stack,
                "key"        : self.device_config.getPostKey( ) }

        respons = requests.post( self.device_config.getPostURL( ) ,
                                 headers=FriskbyClient.headers,
                                 data=json.dumps(data),
                                 timeout=30)
        respons.connection.close()
        if respons.status_code != 201:
            respons.raise_for_status()
            raise Exception('Server did not respond with 201 Created.  Response: %d %s'
                            % (respons.status_code, respons.text))
        return True # no exception, everything written


    def _all_non_uploaded(self):
        q = 'SELECT * FROM samples WHERE NOT uploaded LIMIT=100' # extract 100 to config?
        try:
            conn = sqlite3.connect(self.sql_path)
            result = conn.execute(q)
            return result
        except Exception as e:
            sys.stderr.write('Error on reading data: %s.\n' % e)
        finally:
            conn.close()

    def _mark_uploaded(self, data):
        q = 'UPDATE samples SET uploaded=1 WHERE id=%s' # extract 100 to config?
        try:
            conn = sqlite3.connect(self.sql_path)
            conn.execute('begin')
            for d in data:
                # id, value, sensor, timestamp, uploaded
                id_,_,_,_,_ = d
                conn.execute(q%id_)
            conn.execute('commit')
        except Exception as e:
            sys.stderr.write('Error on setting UPLOADED data! %s.\n' % e)
        finally:
            conn.close()

    def post(self):
        timestamp = dt.utcnow().isoformat() + "+00:00" # manual TZ
        to_upload = self._all_non_uploaded()
        if len(to_upload) <= 4:
            return
        if self._upload(data):
            self._mark_uploaded(to_upload)
