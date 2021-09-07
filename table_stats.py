#!/usr/bin/env python3

import argparse
import configparser
import json

import trino_api
import utils

def calculate_throughput(size, time):
    return (size / time) if time > 0 else 0

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
    for stage in stages:
        stage_plan = json.loads(stage['plan']['jsonRepresentation'])
        # we can only care about scan stages
        if 'scan' not in stage_plan['name'].lower():
            continue
        table_name = utils.get_table_name(stage_plan)
        print (' === Source table - {0} ==='.format(table_name))
        print('  Stage ID is: ', stage['stageId'])
        print('  State      : ', stage['state'])
        print('  type       : ', stage_plan['name'])
        print('  table      : ', stage_plan['identifier'])
        stage_catalog_name = utils.get_catalog_name(stage_plan)
        if stage_catalog_name is None:
            stage_catalog_name = "UNKNOWN"
        stage_stats = stage['stageStats']
        physical_input_stage_throughput = calculate_throughput(
            utils.parse_read_data_size(stage_stats['physicalInputDataSize']),
            utils.parse_read_time(stage_stats['physicalInputReadTime'])
        )
        print('  physical input data size        : ', stage_stats['physicalInputDataSize'])
        print('  physical input read time        : ', stage_stats['physicalInputReadTime'])
        print('  physical input throughput (MB/s): ', physical_input_stage_throughput)
        tasks = stage['tasks']
        for task in tasks:
            worker_host = task['taskStatus']['self'].split("//")[1].split(":")[0]
            print('    === Task {0} on worker {1} ==='.format(task['taskStatus']['taskId'], worker_host))
            task_stats = task['stats']
            physical_input_task_throughput = calculate_throughput(
                utils.parse_read_data_size(task_stats['physicalInputDataSize']),
                utils.parse_read_time(task_stats['physicalInputReadTime'])
            )
            print('    physical input data size        : ', task_stats['physicalInputDataSize'])
            print('    physical input read time        : ', task_stats['physicalInputReadTime'])
            print('    physical input throughput (MB/s): ', physical_input_task_throughput)
    
    session_info = query_json['session']
    print(' === Client Destination - {0} ({1}) ==='.format(session_info['remoteUserAddress'], session_info['source']))
    output_stage_stats = query_json['outputStage']['stageStats']
    print('  buffered                       : ', output_stage_stats['bufferedDataSize'])
    print('  output size                    : ', output_stage_stats['outputDataSize'])
    query_elapsed_time = utils.parse_read_time(query_json['queryStats']['elapsedTime'])
    client_output_throughput = (utils.parse_read_data_size(output_stage_stats['outputDataSize']) / query_elapsed_time) if query_elapsed_time > 0 else 0
    print('  client output throughput (MB/s): ', client_output_throughput)

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
