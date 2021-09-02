#!/usr/bin/env python3

import configparser
import json

from worker import Worker
import utils
import trino_api

workers = {}

def worker_stats(query_json):
    stages = utils.build_stages(query_json)
    for stage in stages:
        stage_plan = json.loads(stage['plan']['jsonRepresentation'])
        stage_catalog_name = utils.get_catalog_name(stage_plan)
        if stage_catalog_name is None:
            stage_catalog_name = "UNKNOWN"
        tasks = stage['tasks']
        for task in tasks:
            worker_host = task['taskStatus']['self'].split("//")[1].split(":")[0]
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
            workers[worker_host] = worker

config = configparser.ConfigParser()
config.read('config.ini')
trino_config = config['trino']

session = trino_api.new_session(trino_config['user'], trino_config['password'])
coordinator_uri = trino_config['http_scheme'] + '://' + trino_config['host'] + ':' + trino_config['port']
verify_certs = trino_config['verify_certs'].lower() == 'true'

for query in trino_api.current_queries(session, verify_certs, coordinator_uri, trino_config['user']):
    worker_stats(trino_api.get_query_json(session, verify_certs, coordinator_uri, trino_config['user'], query['queryId']))

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
