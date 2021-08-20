# read time is a Duration object from Airlift library
# example: 10ns, 10us, 10ms, 10s, 10m, 10h, 10d
# we convert all to milliseconds
def parse_read_time(read_time):
    if "ns" in read_time:
        return float(read_time[:-2]) / 1000000
    if "us" in read_time:
        return float(read_time[:-2]) / 1000
    if "ms" in read_time:
        return float(read_time[:-2])
    if "s" in read_time:
        return float(read_time[:-1]) * 1000
    if "m" in read_time:
        return float(read_time[:-1]) * (1000 * 60)
    if "h" in read_time:
        return float(read_time[:-1]) * (1000 * 60 * 60)
    if "d" in read_time:
        return float(read_time[:-1]) * (1000 * 60 * 60 * 24)
    return 0

# read size is a DataSize object from Airlift library
# example: 10B, 10kB, 10MB, 10GB, 10TB, 10PB
# we convert all to megabytes
def parse_read_data_size(read_size):
    if "PB" in read_size:
        return int(read_size[:-2]) * (1024 * 1024 * 1024)
    if "TB" in read_size:
        return int(read_size[:-2]) * (1024 * 1024)
    if "GB" in read_size:
        return int(read_size[:-2]) * 1024
    if "MB" in read_size:
        return int(read_size[:-2])
    if "kB" in read_size:
        return int(read_size[:-2]) / 1024
    if "B" in read_size:
        return int(read_size[:-1]) / (1024 * 1024)
    return 0

def build_stages(query_json):
    stages = []
    stages.append(query_json['outputStage'])
    def gather_stages(json_stages):
        for json_stage in json_stages:
            stages.append(json_stage)
            gather_stages(json_stage['subStages'])
    gather_stages(query_json['outputStage']['subStages'])
    return stages

def get_catalog_name(stage_plan):
    identifier = stage_plan['identifier']
    parts = identifier.split("table = ")
    if len(parts) > 1:
        return parts[1].split(":")[0]
    if len(stage_plan['children']) > 0:
        for child in stage_plan['children']:
            identifier = child['identifier']
            parts = identifier.split(":")
            if len(parts) > 1:
                return parts[0].replace("[", "").replace("table = ", "")
            else:
                return get_catalog_name(child)