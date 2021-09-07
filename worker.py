class Worker:
    hostname = 'localhost'
    running_splits = 0
    blocked_splits = 0
    total_physical_input = 0
    total_physical_input_read_time = 0
    total_input = 0
    total_physical_output = 0

    def __init__(self, hostname):
        self.hostname = hostname
        self.per_catalog_stats = {}

    def add_running_splits(self, num_of_splits):
        self.running_splits += num_of_splits

    def add_blocked_splits(self, num_of_splits):
        self.blocked_splits += num_of_splits
    
    def add_physical_input(self, catalog_name, physical_input):
        self.total_physical_input += physical_input
        self.inc_catalog_stats(catalog_name, physical_input = physical_input)
    
    def add_physical_input_read_time(self, catalog_name, physical_input_read_time):
        self.total_physical_input_read_time += physical_input_read_time
        self.inc_catalog_stats(catalog_name, physical_input_read_time = physical_input_read_time)
    
    def add_input(self, catalog_name, input):
        self.total_input += input
        self.inc_catalog_stats(catalog_name, input = input)

    def add_physical_output(self, catalog_name, physical_output):
        self.total_physical_output += physical_output
        self.inc_catalog_stats(catalog_name, physical_output = physical_output)
    
    def get_catalog_stats(self, catalog_name):
        if self.per_catalog_stats.get(catalog_name) is None:
            self.per_catalog_stats[catalog_name] = {
                'total_physical_input': 0,
                'total_physical_input_read_time': 0,
                'total_input': 0,
                'total_physical_output': 0
            }
        return self.per_catalog_stats.get(catalog_name)
    
    def get_overall_physical_input_throughput(self):
        if self.total_physical_input_read_time == 0:
            return 0
        return self.total_physical_input / self.total_physical_input_read_time

    def get_physical_input_throughput(self, catalog_name):
        catalog_stats = self.get_catalog_stats(catalog_name)
        if catalog_stats['total_physical_input_read_time'] == 0:
            return 0
        return catalog_stats['total_physical_input'] / catalog_stats['total_physical_input_read_time']

    def inc_catalog_stats(self, catalog_name, physical_input = 0, physical_input_read_time = 0, input = 0, physical_output = 0):
        catalog_stats = self.get_catalog_stats(catalog_name)
        catalog_stats['total_physical_input'] += physical_input
        catalog_stats['total_physical_input_read_time'] += physical_input_read_time
        catalog_stats['total_input'] += input
        catalog_stats['total_physical_output'] += physical_output
        self.per_catalog_stats[catalog_name] = catalog_stats
