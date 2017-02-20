import json
import requests

from friskby_dao import FriskbyDao



class FriskbySubmitter(object):
    """This class submits data from a database to the friskby cloud, then proceeds
    to mark the uploaded data as such.  The constructor takes a device_config
    (to read https keys and urls) and a path to an sqlite file.
    """

    def __init__(self, device_config, sql_path):
        self.device_config = device_config
        self.dao = FriskbyDao(sql_path)

    def _upload(self, rows):
        # id, value, sensor, timestamp, uploaded
        data = {}
        for row in rows:
            id_, value, sensor, timestamp, _ = row
            if sensor not in data:
                data[sensor] = []
            data[sensor].append((id_, value, sensor, timestamp))

        for sensor in data:
            push = {"sensorid"   : sensor,
                    "value_list" : data[sensor],
                    "key"        : self.device_config.getPostKey()}
            respons = requests.post(self.device_config.getPostURL(),
                                    headers={'Content-Type': 'application/json'},
                                    data=json.dumps(push),
                                    timeout=30)
        respons.connection.close()
        if respons.status_code != 201:
            respons.raise_for_status()
            raise Exception('Server did not respond with 201 Created.  Response: %d %s'
                            % (respons.status_code, respons.text))
        return True # no exception, everything written

    def post(self):
        to_upload = self.dao.get_non_uploaded()
        if len(to_upload) <= 4:
            return
        if self._upload(to_upload):
            self.dao.mark_uploaded(to_upload)
