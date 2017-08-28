import requests
import json

HOST='http://127.0.0.1:8080'

class TestGet(object):
    def test_get_tasks(self):
        test_data = {"input": "input.txt", "map": "map.bin", "reduce": "reduce.bin"}
        check_data = '{"test": {"input": "input.txt", "map": "map.bin", "reduce": "reduce.bin"}}'
        post = requests.post(HOST + '/api/add_task/test', json=test_data)
        r = requests.get(HOST + '/api/get_tasks') 
        print(r.json())
        print(check_data)
        assert r.json() == check_data
