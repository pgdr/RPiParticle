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


    def post(self):
        timestamp = dt.utcnow().isoformat() + "+00:00" # manual TZ
        to_upload = self._all_non_uploaded()
        if len(to_upload) <= 4:
            return
        if self._upload(data):
            self._mark_uploaded(to_upload)

if __name__ == '__main__':
    # read sys.argv
    submitter = FriskbySubmitter( device_config , sql_path) # todo read device config
    submitter.post()
