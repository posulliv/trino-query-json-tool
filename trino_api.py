import requests

def new_session(trino_user, trino_password):
    session = requests.Session()
    session.auth = (trino_user, trino_password)
    return session

def current_queries(session, verify_certs, coordinator_uri, trino_user):
    queries_url = coordinator_uri + '/v1/queryState'
    return session.get(queries_url, verify=verify_certs, headers = {'X-Trino-User' : trino_user}).json()

def get_query_json(session, verify_certs, coordinator_uri, trino_user, query_id):
    query_json_url = coordinator_uri + '/v1/query/' + query_id
    return session.get(query_json_url, verify=verify_certs, headers = {'X-Trino-User' : trino_user}).json()