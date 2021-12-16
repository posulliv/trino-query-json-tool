import requests

def new_session(trino_user, trino_password):
    session = requests.Session()
    if trino_password:
        session.auth = (trino_user, trino_password)
    return session

def current_queries(session, verify_certs, coordinator_uri, trino_user):
    queries_url = coordinator_uri + '/v1/queryState'
    return session.get(queries_url, verify=verify_certs, headers = {'X-Trino-User' : trino_user}).json()

def get_query_json(session, verify_certs, coordinator_uri, trino_user, query_id):
    query_json_url = coordinator_uri + '/v1/query/' + query_id
    return session.get(query_json_url, verify=verify_certs, headers = {'X-Trino-User' : trino_user}).json()

def get_resource_group_state(session, verify_certs, coordinator_uri, trino_user, resource_group_id):
    resource_group_state_url = coordinator_uri + '/v1/resourceGroupState/' + resource_group_id
    return session.get(resource_group_state_url, verify=verify_certs, headers = {'X-Trino-User' : trino_user}).json()