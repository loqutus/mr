#!/usr/bin/env python3

from flask import Flask, request
from paramiko import SSHClient, AutoAddPolicy
from os.path import isfile
import json

data_dir = '/home/rusik/mr/data/'
input_dir = data_dir + 'input/'
split_dir = data_dir + 'split_input/'
map_dir = data_dir + 'map/'
reduce_dir = data_dir + 'reduce/'

app = Flask(__name__)
tasks = {}
hosts = {}
host_counter = 0
tasks_counter = 0


def split_input(input_name):
    print('split_input ' + input_name)
    with open(data_dir + 'input/' + input_name, 'rb') as input_file:
        buffer_size = 1
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
                split_name = data_dir + 'split_input/' + input_name + '.' + str(host_number)
                if not isfile(split_name):
                    with open(split_name, 'w') as split_file:
                        for line in buffer:
                            print(line, file=split_file)
                    hosts_counter += 1
                    buffer = []
                    print('split ' + split_name + ' written')
                else:
                    print('split ' + split_name + ' already exists')


def sftp_copy_to(local_file, remote_file, host, user):
    with SSHClient() as ssh:
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(host, 22, user)
        ftp_client = ssh.open_sftp()
        ftp_client.put(local_file, remote_file)
        ftp_client.close()


def sftp_copy_from(remote_file, local_file, host, user):
    with SSHClient as ssh:
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(host, 22, user)
        ftp_client = ssh.open_sftp()
        ftp_client.get(remote_file, local_file)
        ftp_client.close()


def copy_to_hosts(task):
    print('copy to hosts')
    for host, host_params in hosts.items():
        input_split = task['input'] + '.' + str(host_params['id'])
        sftp_copy_to(split_dir + input_split, input_dir + task['input'], host, host_params['user'])
        sftp_copy_to(map_dir + task['map'], map_dir + task['map'], host, host_params['user'])
        sftp_copy_to(reduce_dir + task['reduce'], map_dir + task['reduce'], host,
                     host_params['user'])


def get_output(task):
    print('get output')
    for host, host_params in hosts.items():
        input_split = task['input'] + '.' + str(host_params['id'])
        sftp_copy_from()


def ssh_run(task, input_name, output, host, user):
    with SSHClient() as ssh:
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(host, 22, user)
        command = '/usr/bin/env python {task} {input} {output}'.format(task=task, input=input_name, output=output)
        (stdin, stdout, stderr) = ssh.exec_command(command)
        ssh.close()


def run_task(task):
    print('run task')
    for host, host_params in hosts.items():
        ssh_run(task, host, host_params['user'])


# add task to tasks dict
# {"input": "input.txt", "map": "map.bin", "reduce": "reduce.bin"}
@app.route('/api/add_task/<task_name>', methods=['POST'])
def add_task(task_name):
    if task_name not in tasks:
        global tasks_counter
        tasks_counter += 1
        task_map = request.json['map']
        task_reduce = request.json['reduce']
        task_input = request.json['input']
        tasks[task_name] = {'map': task_map, 'reduce': task_reduce, 'input': task_input, 'id': tasks_counter}
        print('task ' + task_name + ' added')
        return 'ok', 200
    else:
        return 'task already exists', 409


# get tasks dict
@app.route('/api/get_tasks', methods=['GET'])
def get_tasks():
    print('get tasks')
    return json.dumps(tasks), 200


# add host to hosts dict
# {"dir": "/home/rusik/mr", "user": "rusik"}
@app.route('/api/add_host/<host_name>', methods=['POST'])
def add_host(host_name):
    if host_name not in hosts:
        global host_counter
        host_counter += 1
        host_dir = request.json['dir']
        host_user = request.json['user']
        hosts[host_name] = {'dir': host_dir, 'user': host_user, 'id': host_counter}
        print('host ' + host_name + ' added')
        return 'ok', 200
    else:
        return 'host already exists', 409


# get hosts dict
@app.route('/api/get_hosts', methods=['GET'])
def get_hosts():
    print('get hosts')
    return json.dumps(hosts), 200


# add map
@app.route('/api/add_map/<map_name>', methods=['POST'])
def add_map(map_name):
    file_name = data_dir + 'map/' + map_name
    if not isfile(file_name):
        with open(file_name, 'wb') as f:
            f.write(request.data)
        print('map ' + map_name + ' added')
        return 'ok', 200
    else:
        return 'map already exists', 409


# add reduce
@app.route('/api/add_reduce/<reduce_name>', methods=['POST'])
def add_reduce(reduce_name):
    file_name = data_dir + 'reduce/' + reduce_name
    if not isfile(file_name):
        with open(file_name, 'wb') as f:
            f.write(request.data)
        print('reduce ' + reduce_name + ' added')
        return 'ok', 200
    else:
        return 'reduce already exists', 409


# add input
@app.route('/api/add_input/<input_name>', methods=['POST'])
def add_input(input_name):
    file_name = data_dir + 'input/' + input_name
    if not isfile(file_name):
        with open(file_name, 'wb') as f:
            f.write(request.data)
        print('input ' + input_name + ' added')
        return 'ok', 200
    else:
        return 'reduce already exists', 409


# run task
@app.route('/api/run_task/<task_name>', methods=['GET'])
def run_task(task_name):
    print('running task ' + task_name)
    split_input(tasks[task_name]['input'])
    copy_to_hosts(tasks[task_name])
    run_task(tasks[task_name])
    get_output(tasks[task_name])
    del tasks[task_name]
    return 'ok', 200


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
