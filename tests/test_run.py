import requests

HOST = 'http://127.0.0.1:8080'


class TestRun(object):
    def test_run_task(self):
        r = requests.get(HOST + '/api/run_task/test')
        assert r.status_code == 200
