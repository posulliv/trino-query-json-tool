#!/usr/bin/env python3

import argparse
import configparser
import json

import trino_api
import utils

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
            task_stats = task['stats']
            print('    physical input data size: ', task_stats['physicalInputDataSize'])
            print('    physical input read time: ', task_stats['physicalInputReadTime'])
            print('    total splits            : ', task_stats['totalDrivers'])
            print('    completed splits        : ', task_stats['completedDrivers'])
            print('    running splits          : ', task_stats['runningDrivers'])
            print('    queued splits           : ', task_stats['queuedDrivers'])
            print('    blocked splits          : ', task_stats['blockedDrivers'])

parser = argparse.ArgumentParser(description='Accepted parameters')
parser.add_argument('-q', '--query', help='query ID', required=True)
args = parser.parse_args()

config = configparser.ConfigParser()
config.read('config.ini')
trino_config = config['trino']

session = trino_api.new_session(trino_config['user'], trino_config['password'])
coordinator_uri = trino_config['http_scheme'] + '://' + trino_config['host'] + ':' + trino_config['port']
verify_certs = trino_config['verify_certs'].lower() == 'true'

parse_query_json(
    trino_api.get_query_json(session, verify_certs, coordinator_uri, trino_config['user'], args.query)
)
