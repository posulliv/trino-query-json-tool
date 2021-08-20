#!/usr/bin/env python3

import json
import requests
import configparser

from worker import Worker
import utils

workers = {}

def parse_query_json(query_json):
    query_stats = query_json['queryStats']
    print('=== Query Info ===')
    print('query ID        : ', query_json['queryId'])
    print('state           : ', query_json['state'])
    print('elapsed time    : ', query_stats['elapsedTime'])
    print('total splits    : ', query_stats['totalDrivers'])
    print('completed splits: ', query_stats['completedDrivers'])
    print('running splits  : ', query_stats['runningDrivers'])
    print('queued splits   : ', query_stats['queuedDrivers'])
    print('blocked splits  : ', query_stats['blockedDrivers'])
    catalogs = set()
    stages = utils.build_stages(query_json)
    print('  === Stage Stats ===')
    for stage in stages:
        print('  Stage ID is: ', stage['stageId'])
        print('  State      : ', stage['state'])
        stage_plan = json.loads(stage['plan']['jsonRepresentation'])
        print('  type       : ', stage_plan['name'])
        print('  table      : ', stage_plan['identifier'])
        stage_catalog_name = utils.get_catalog_name(stage_plan)
        if stage_catalog_name is None:
            stage_catalog_name = "UNKNOWN"
        catalogs.add(stage_catalog_name)
        stage_stats = stage['stageStats']
        print('  physical input data size: ', stage_stats['physicalInputDataSize'])
        print('  physical input read time: ', stage_stats['physicalInputReadTime'])
        print('  total splits            : ', stage_stats['totalDrivers'])
        print('  completed splits        : ', stage_stats['completedDrivers'])
        print('  running splits          : ', stage_stats['runningDrivers'])
        print('  queued splits           : ', stage_stats['queuedDrivers'])
        print('  blocked splits          : ', stage_stats['blockedDrivers'])
        tasks = stage['tasks']
        print('    === Task Stats ===')
        for task in tasks:
            print('    Task ID is: ', task['taskStatus']['taskId'])
            worker_host = task['taskStatus']['self'].split("//")[1].split(":")[0]
            print('    worker is : ', worker_host)
            worker = workers.get(worker_host)
            if worker is None:
                worker = Worker(worker_host)
            task_stats = task['stats']
            worker.add_running_splits(task_stats['runningDrivers'])
            worker.add_blocked_splits(task_stats['blockedDrivers'])
            worker.add_physical_input(stage_catalog_name, utils.parse_read_data_size(
                task_stats['physicalInputDataSize'])
            )
            worker.add_physical_input_read_time(
                stage_catalog_name, utils.parse_read_time(task_stats['physicalInputReadTime'])
            )
            worker.add_input(stage_catalog_name, utils.parse_read_data_size(
                task_stats['rawInputDataSize'])
            )
            print('    physical input data size: ', task_stats['physicalInputDataSize'])
            print('    physical input read time: ', task_stats['physicalInputReadTime'])
            print('    total splits            : ', task_stats['totalDrivers'])
            print('    completed splits        : ', task_stats['completedDrivers'])
            print('    running splits          : ', task_stats['runningDrivers'])
            print('    queued splits           : ', task_stats['queuedDrivers'])
            print('    blocked splits          : ', task_stats['blockedDrivers'])
            workers[worker_host] = worker

config = configparser.ConfigParser()
config.read('config.ini')
trino_config = config['trino']

session = requests.Session()
session.auth = (trino_config['user'], trino_config['password'])
coordinator_uri = trino_config['http_scheme'] + '://' + trino_config['host'] + ':' + trino_config['port']
queries_url = coordinator_uri + '/v1/queryState'
query_url = coordinator_uri + '/v1/query/'
verify_certs = trino_config['verify_certs'].lower() == 'true'

queries = session.get(queries_url, verify=verify_certs, headers = {'X-Trino-User' : trino_config['user']}).json()
for query in queries:
    query_json_url = query_url + query['queryId']
    query_json = session.get(query_json_url, verify=verify_certs, headers = {'X-Trino-User' : trino_config['user']}).json()
    parse_query_json(query_json)

print (' === Per Worker Stats === ')

for worker_host in sorted(workers.keys()):
    worker = workers.get(worker_host)
    print('worker                    : ', worker.hostname)
    print('  running splits          : ', worker.running_splits)
    print('  blocked splits          : ', worker.blocked_splits)
    print('  physical input data size: ', worker.total_physical_input)
    print('  physical input read time: ', worker.total_physical_input_read_time)
    print('  == catalog stats ==')
    for catalog_name in sorted(worker.per_catalog_stats.keys()):
        catalog_stats = worker.per_catalog_stats.get(catalog_name)
        print('    catalog                   : ', catalog_name)
        print('      physical input data size: ', catalog_stats['total_physical_input'])
        print('      physical input read time: ', catalog_stats['total_physical_input_read_time'])
        print('      input data size         : ', catalog_stats['total_input'])
