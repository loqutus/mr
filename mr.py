#!/usr/bin/env python3

from flask import Flask, request
import json

data_dir = '/home/rusik/mr/data/'

app = Flask(__name__)
tasks = {}
hosts = {}

# add task to tasks dict
# {"input": "input.txt", "map": "map.bin", "reduce": "reduce.bin"}
@app.route('/api/add_task/<task_name>', methods=['POST'])
def add_task(task_name):
    task_map = request.json['map']
    task_reduce = request.json['reduce']
    task_input = request.json['input']
    tasks[task_name] = {'map': task_map, 'reduce': task_reduce, 'input': task_input} 
    print('task ' + task_name + ' added')
    return 'ok', 200

# get tasks dict
@app.route('/api/get_tasks', methods=['GET'])
def get_tasks():
    print('get tasks')
    return json.dumps(tasks), 200


# add host to hosts dict
# {"dir": "/home/pi/mr", "user": "pi"}
@app.route('/api/add_host/<host_name>', methods=['POST'])
def add_host(host_name):
    host_dir = request.json['dir']
    host_user = request.json['user']
    hosts[host_name] = {'dir': host_dir, 'user': task_user} 
    print('host ' + host_name + ' added')
    return 'ok', 200

# get hosts dict
@app.route('/api/get_hosts', methods=['GET'])
def get_hosts():
    print('get hosts')
    return json.dumps(hosts), 200

# add map
@app.route('/api/add_map/<map_name>', methods=['POST'])
def add_map(map_name):
    with open(data_dir + 'map/' + map_name, 'wb') as f:
        f.write(request.data)
    print('map ' + map_name + ' added')
    return 'ok', 200

# add reduce
@app.route('/api/add_reduce/<reduce_name>', methods=['POST'])
def add_reduce(reduce_name):
    with open(data_dir + 'reduce/' + reduce_name, 'wb') as f:
        f.write(request.data)
    print('reduce ' + reduce_name + ' added')
    return 'ok', 200

# add input
@app.route('/api/add_input/<input_name>', methods=['POST'])
def add_input(input_name):
    with open(data_dir + 'input/' + input_name, 'wb') as f:
        f.write(request.data)
    print('input ' + input_name + ' added')
    return 'ok', 200


if __name__ == '__main__':
    app.run(host = '0.0.0.0', port=8080, debug=True)
