import json
import requests

class FriskbySubmitter(object):
    """This class submits data from a database to the friskby cloud, then proceeds
    to mark the uploaded data as such.

    """

    def __init__(self, dao):
        self.dao = dao

    def set_config(self, device_config):
        self.device_config = device_config

    def get_dao(self):
        return self.dao

    def _upload(self, rows):
        print('Attempting to upload.')
        # id, value, sensor, timestamp, uploaded
        data = {}
        for row in rows:
            id_, value, sensor, timestamp, _ = row
            if sensor not in data:
                data[sensor] = []
            data[sensor].append((id_, value, sensor, timestamp))

        print('Connecting.')
        for sensor in data:
            push = {"sensorid"   : sensor,
                    "value_list" : data[sensor],
                    "key"        : self.device_config.getPostKey()}
            respons = requests.post(self.device_config.getPostURL(),
                                    headers={'Content-Type': 'application/json'},
                                    data=json.dumps(push),
                                    timeout=30)
            print('posted %s' % sensor)
            if respons.status_code != 201:
                respons.raise_for_status()
                raise Exception('Server did not respond with 201 Created.  Response: %d %s'
                                % (respons.status_code, respons.text))
            respons.connection.close()
        return True # no exception, everything written

    def post(self):
        if self.device_config is None:
            raise ValueError('Device config not set!')
        to_upload = self.dao.get_non_uploaded()
        if self._upload(to_upload):
            self.dao.mark_uploaded(to_upload)
