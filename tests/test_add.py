import requests

HOST='http://127.0.0.1:8080'

class TestAdd(object):
    def test_add_input(self):
        r=requests.post(HOST + '/api/add_input/test', data='a\nb\nc\n') 
        assert 'ok' in r.text
        with open('data/input/test') as f:
            assert 'a\nb\nc\n' == f.read()

    def test_add_map(self):
        r=requests.post(HOST + '/api/add_map/test', data='a\nb\nc\n') 
        assert 'ok' in r.text
        with open('data/map/test') as f:
            assert 'a\nb\nc\n' == f.read()

    def test_add_reduce(self):
        r=requests.post(HOST + '/api/add_reduce/test', data='a\nb\nc\n') 
        assert 'ok' in r.text
        with open('data/reduce/test') as f:
            assert 'a\nb\nc\n' == f.read()
