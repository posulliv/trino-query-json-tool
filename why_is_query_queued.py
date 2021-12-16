#!/usr/bin/env python3

import argparse
import configparser
import json

import trino_api
import utils

def get_resource_group_id(query_json):
    print('=== Query Info ===')
    print('query ID          : ', query_json['queryId'])
    print('state             : ', query_json['state'])
    print('resource group ID : ', query_json['resourceGroupId'])
    print('resource estimates: ', query_json['session']['resourceEstimates'])
    print('elapsed time      : ', query_json['queryStats']['elapsedTime'])
    print('queued time       : ', query_json['queryStats']['queuedTime'])
    # need to get root group ID from resource group info in the JSON
    return query_json['resourceGroupId']

def find_matching_resource_group(resource_group_id, resource_group):
    if resource_group_id == resource_group['id']:
        return resource_group
    for sub_group in resource_group['subGroups']:
        return find_matching_resource_group(resource_group_id, sub_group)

def find_running_queries_in_resource_group(resource_group_id, running_queries):
    queries = []
    for query in running_queries:
        if resource_group_id == query['resourceGroupId']:
            queries.append(query)
    return queries;

def print_resource_group_limits(resource_group, resource_group_state):
    print('=== Resource Group Limits ===')
    print('  ID                    : ', resource_group['id'])
    print('  memory limit          : ', resource_group['softMemoryLimit'])
    print('  soft concurrency limit: ', resource_group['softConcurrencyLimit'])
    print('  hard concurrency limit: ', resource_group['hardConcurrencyLimit'])
    print('  max queued queries    : ', resource_group['maxQueuedQueries'])
    print('=== Resource Group Current Usage ===')
    print('  num of running queries: ', resource_group['numRunningQueries'])
    print('  num of queued queries : ', resource_group['numQueuedQueries'])
    print('  CPU usage             : ', resource_group['cpuUsage'])
    print('  memory usage          : ', resource_group['memoryUsage'])
    print('  === Running Queries ===')
    # need to get from the entire resource group state
    queries = find_running_queries_in_resource_group(resource_group['id'], resource_group_state['runningQueries'])
    for query in queries:
        print('    ID : ', query['queryId'])
    
    # TODO
    # print reasoning on Why is Query Queued?
    # if number of running queries is equal to hard concurrency limit, say that here
    # if memory usage is greater than softMemoryLimit, say that here
    #TODO - scheduling weight needs to be taken into account and priority

parser = argparse.ArgumentParser(description='Accepted parameters')
parser.add_argument('-q', '--query', help='query ID', required=True)
args = parser.parse_args()

config = configparser.ConfigParser()
config.read('config.ini')
trino_config = config['trino']
use_password = True if trino_config['http_scheme'] == 'https' else False

session = trino_api.new_session(trino_config['user'], trino_config['password'] if use_password else None)
coordinator_uri = trino_config['http_scheme'] + '://' + trino_config['host'] + ':' + trino_config['port']
verify_certs = trino_config['verify_certs'].lower() == 'true'

resource_group_id = get_resource_group_id(
    trino_api.get_query_json(session, verify_certs, coordinator_uri, trino_config['user'], args.query)
)

resource_group_state = trino_api.get_resource_group_state(
    session, verify_certs, coordinator_uri, trino_config['user'], resource_group_id[0]
)
resource_group = find_matching_resource_group(resource_group_id, resource_group_state)
print_resource_group_limits(resource_group, resource_group_state)