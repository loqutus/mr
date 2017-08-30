import requests
import json

HOST = 'http://127.0.0.1:8080'


class TestGet(object):
    def test_get_tasks(self):
        test_data = {"input": "test", "map": "map.bin", "reduce": "reduce.bin"}
        check_data = '{"test": {"input": "test", "map": "map.bin", "reduce": "reduce.bin", "id": 1}}'
        post = requests.post(HOST + '/api/add_task/test', json=test_data)
        assert post.status_code == 200
        r = requests.get(HOST + '/api/get_tasks')
        assert r.json() == json.loads(check_data)

    def test_get_hosts(self):
        test_data = {"dir": "/home/rusik/mr/data", "user": "rusik", "id": 1}
        check_data = '{"localhost": {"dir": "/home/rusik/mr/data", "user": "rusik", "id":1}}'
        post = requests.post(HOST + '/api/add_host/localhost', json=test_data)
        assert post.status_code == 200
        r = requests.get(HOST + '/api/get_hosts')
        assert r.json() == json.loads(check_data)
