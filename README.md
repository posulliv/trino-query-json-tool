# Overview

This repo contains some simple python scripts for various tasks when
working with a Trino cluster.

## worker_stats.py

First, it calls `/v1/queryState` to get the list of running queries and then for each query, 
it calls `/v1/query/<queryId>` to get the query JSON for an individual query. The information
it reports is extracted from the query JSON.

The metrics are output on a per-worker basis.

## query_stats.py

This script takes a query ID as an argument and calls `/v1/query/<queryId>` to get the query
JSON for the query. The information it reports is extracted from the query JSON.

## why_is_query_queued.py

This script takes a query ID as an argument and gets resource group info for the resource
group the query is using. It uses the `/v1/resourceGroupState/<resourceGroupId>` endpoint
to get resource group state. 

Using the info printed for the resource group, you can see what resources in the group are
causing the query to be queued.

# Requirements

* python3
* requests library (`python3 -m pip install requests`)

# Examples

To run any script, first update `config.ini` with values for your cluster. An example config for a cluster running locally:

```
[trino]
port=8443
http_scheme=https
host=localhost
user=bob
password=bob
verify_certs=false
```

With that in place, run one of the scripts simply with: `./worker_stats.py`

You will see output similar to:

```
=== Query Info ===
query ID        :  20210820_200354_56023_2v7uu
state           :  RUNNING
elapsed time    :  5.99s
total splits    :  33
completed splits:  0
running splits  :  16
queued splits   :  0
blocked splits  :  17
  === Stage Stats ===
  Stage ID is:  20210820_200354_56023_2v7uu.0
  State      :  RUNNING
  type       :  Output
  table      :  [_col0]
  physical input data size:  0B
  physical input read time:  0.00ns
  total splits            :  17
  completed splits        :  0
  running splits          :  0
  queued splits           :  0
  blocked splits          :  17
    === Task Stats ===
    Task ID is:  20210820_200354_56023_2v7uu.0.0
    worker is :  192.168.86.201
    physical input data size:  0B
    physical input read time:  0.00ns
    total splits            :  17
    completed splits        :  0
    running splits          :  0
    queued splits           :  0
    blocked splits          :  17
  Stage ID is:  20210820_200354_56023_2v7uu.1
  State      :  RUNNING
  type       :  Aggregate(PARTIAL)
  table      :
  physical input data size:  0B
  physical input read time:  0.00ns
  total splits            :  16
  completed splits        :  0
  running splits          :  16
  queued splits           :  0
  blocked splits          :  0
    === Task Stats ===
    Task ID is:  20210820_200354_56023_2v7uu.1.0
    worker is :  192.168.86.201
    physical input data size:  0B
    physical input read time:  0.00ns
    total splits            :  16
    completed splits        :  0
    running splits          :  16
    queued splits           :  0
    blocked splits          :  0
 === Per Worker Stats ===
worker                    :  192.168.86.201
  running splits          :  16
  blocked splits          :  17
  physical input data size:  0.0
  physical input read time:  0.0
  == catalog stats ==
    catalog                   :  UNKNOWN
      physical input data size:  0.0
      physical input read time:  0.0
      input data size         :  0.0
    catalog                   :  tpch
      physical input data size:  0.0
      physical input read time:  0.0
      input data size         :  0.0
```

If you have a large cluster with a lot of queries running, output will be large.

# Future Work

* Calculate throughput numbers per catalog
* Add different output options so script can be used to gather monitoring metrics 
* Deeper analysis on individual query
* More robust catalog name detection
* General code cleanup