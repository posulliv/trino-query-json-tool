#!/usr/bin/env python3

import configparser

import utils
import trino_api

config = configparser.ConfigParser()
config.read('config.ini')
trino_config = config['trino']

session = trino_api.new_session(trino_config['user'], trino_config['password'])
coordinator_uri = trino_config['http_scheme'] + '://' + trino_config['host'] + ':' + trino_config['port']
verify_certs = trino_config['verify_certs'].lower() == 'true'

def check_output_buffers(query_json):
    output_stage_stats = query_json['outputStage']['stageStats']
    buffered_size = utils.parse_read_data_size(output_stage_stats['bufferedDataSize'])
    if (buffered_size >= 32):
        print('(WARNING) root stage output buffer full for query: ', query_json['queryId'])
    print('=== output stats ===')
    print('  buffered   : ', output_stage_stats['bufferedDataSize'])
    print('  output size: ', output_stage_stats['outputDataSize'])

for query in trino_api.current_queries(session, verify_certs, coordinator_uri, trino_config['user']):
    query_json = trino_api.get_query_json(session, verify_certs, coordinator_uri, trino_config['user'], query['queryId'])
    if query_json['state'] == 'RUNNING':
        check_output_buffers(query_json)