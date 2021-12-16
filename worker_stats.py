#!/usr/bin/env python3

import argparse
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
            worker.add_physical_output(
                stage_catalog_name,
                utils.parse_read_data_size(task_stats['physicalWrittenDataSize'])
            )
            workers[worker_host] = worker


def console_stats_output(workers):
    print (' === Per Worker Stats === ')

    for worker_host in sorted(workers.keys()):
        worker = workers.get(worker_host)
        print('worker                     : ', worker.hostname)
        print('  running splits           : ', worker.running_splits)
        print('  blocked splits           : ', worker.blocked_splits)
        print('  physical input data size : ', worker.total_physical_input)
        print('  physical input read time : ', worker.total_physical_input_read_time)
        print('  physical input throughput: ', worker.get_overall_physical_input_throughput())
        print('  physical output data size: ', worker.total_physical_output)
        print('  == catalog stats ==')
        for catalog_name in sorted(worker.per_catalog_stats.keys()):
            catalog_stats = worker.per_catalog_stats.get(catalog_name)
            print('    catalog                    : ', catalog_name)
            print('      physical input data size : ', catalog_stats['total_physical_input'])
            print('      physical input read time : ', catalog_stats['total_physical_input_read_time'])
            print('      physical input throughput: ', worker.get_physical_input_throughput(catalog_name))
            print('      input data size          : ', catalog_stats['total_input'])
            print('      physical output data size: ', catalog_stats['total_physical_output'])


def prometheus_stats_output(workers):
    print('this will be prometheus')

def output_stats(option, workers):
    if option == 'console':
        console_stats_output(workers)
    elif option == 'prometheus':
        prometheus_stats_output(workers)
    else:
        print ('Invalid output option: ', option)


parser = argparse.ArgumentParser(description='Accepted parameters')
parser.add_argument('-o', '--output', help='Where stats are output', default = 'console')
args = parser.parse_args()

config = configparser.ConfigParser()
config.read('config.ini')
trino_config = config['trino']
use_password = True if trino_config['http_scheme'] == 'https' else False

session = trino_api.new_session(trino_config['user'], trino_config['password'] if use_password else None)
coordinator_uri = trino_config['http_scheme'] + '://' + trino_config['host'] + ':' + trino_config['port']
verify_certs = trino_config['verify_certs'].lower() == 'true'

for query in trino_api.current_queries(session, verify_certs, coordinator_uri, trino_config['user']):
    worker_stats(trino_api.get_query_json(session, verify_certs, coordinator_uri, trino_config['user'], query['queryId']))

output_stats(args.output, workers)