#!/usr/bin/env python3

from flask import Flask, request
from threading import Thread
from time import sleep
from paramiko import SSHClient, AutoAddPolicy
import sys
import json

data_dir = '/home/rusik/mr/data/'

app = Flask(__name__)
tasks = {}
hosts = {}
host_counter = 0
tasks_counter = 0


def split_input(input_name):
    print('split_input')
    with open(data_dir + 'input/' + input_name) as input_file:
        buffer_size = 10
        hosts_counter = 1
        buffer = []
        lines_counter = 0
        hosts_count = len(hosts)
        for input_line in input_file:
            if lines_counter < buffer_size:
                buffer += input_line
                lines_counter += 1
            else:
                host_number = hosts_count % hosts_counter
                with open(data_dir + 'split_input/' + input_name + '.' + str(host_number), 'wb') as split_file:
                    for line in buffer:
                        split_file.write(line)
                hosts_counter += 1
                buffer = []


def sftp_copy(local_filename, host, remote_dir, remote_filename, user):
    with SSHClient() as ssh:
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(host, 22, user)
        ftp_client = ssh.open_sftp()
        ftp_client.put(data_dir + local_filename, remote_dir + '/' + remote_filename)
        ftp_client.close()


def copy_to_hosts(task_name):
    print('copy_to_hosts')
    task_input = tasks[task_name]['input']
    task_map = tasks[task_name]['map']
    task_reduce = tasks[task_name]['reduce']
    for host, host_params in hosts.items():
        host_input_split = task_input + '.' + str(host_params['id'])
        directory = host_params['dir']
        user = host_params['user']
        sftp_copy('split_input/' + host_input_split, host, directory, 'input/' + task_input, user)
        sftp_copy('map/' + task_map, host, directory, 'map/' + task_map, user)
        sftp_copy('reduce/' + task_reduce, host, directory, 'reduce/' + task_input, user)


# add task to tasks dict
# {"input": "input.txt", "map": "map.bin", "reduce": "reduce.bin"}
@app.route('/api/add_task/<task_name>', methods=['POST'])
def add_task(task_name):
    global tasks_counter
    tasks_counter += 1
    task_map = request.json['map']
    task_reduce = request.json['reduce']
    task_input = request.json['input']
    tasks[task_name] = {'map': task_map, 'reduce': task_reduce, 'input': task_input, 'id': tasks_counter}
    print('task ' + task_name + ' added')
    return 'ok', 200


# get tasks dict
@app.route('/api/get_tasks', methods=['GET'])
def get_tasks():
    print('get tasks')
    return json.dumps(tasks), 200


# add host to hosts dict
# {"dir": "/home/rusik/mr", "user": "rusik"}
@app.route('/api/add_host/<host_name>', methods=['POST'])
def add_host(host_name):
    global host_counter
    host_counter += 1
    host_dir = request.json['dir']
    host_user = request.json['user']
    hosts[host_name] = {'dir': host_dir, 'user': host_user, 'id': host_counter}
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


# run task
@app.route('/api/run_task/<task_name>', methods=['GET'])
def run_task(task_name):
    print('running task ' + task_name)
    split_input(tasks[task_name]['input'])
    copy_to_hosts(task_name)
    del tasks[task_name]
    return 'ok', 200


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
