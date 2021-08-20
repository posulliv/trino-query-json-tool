# Overview

This simple python script extracts metrics from a Trino cluster.

First, it calls `/v1/queryState` to get the list of running queries and then for each query, 
it calls `/v1/query/<queryId>` to get the query JSON for an individual query. The information
it reports is extracted from the query JSON.

# Requirements

* python3
* requests library (`python3 -m pip install requests`)

# Example

To run, first update `config.ini` with values for your cluster. An example config for a cluster running locally:

```
[trino]
port=8443
http_scheme=https
host=localhost
user=bob
password=bob
verify_certs=false
```

With that in place, run it simply with: `./parse.py`

Will see output similar to:

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