class Worker:
    hostname = 'localhost'
    running_splits = 0
    blocked_splits = 0
    total_physical_input = 0
    total_physical_input_read_time = 0
    total_input = 0

    def __init__(self, hostname):
        self.hostname = hostname
        self.per_catalog_stats = {}

    def add_running_splits(self, num_of_splits):
        self.running_splits += num_of_splits

    def add_blocked_splits(self, num_of_splits):
        self.blocked_splits += num_of_splits
    
    def add_physical_input(self, catalog_name, physical_input):
        self.total_physical_input += physical_input
        self.inc_catalog_stats(catalog_name, physical_input, 0, 0)
    
    def add_physical_input_read_time(self, catalog_name, physical_input_read_time):
        self.total_physical_input_read_time += physical_input_read_time
        self.inc_catalog_stats(catalog_name, 0, physical_input_read_time, 0)
    
    def add_input(self, catalog_name, input):
        self.total_input += input
        self.inc_catalog_stats(catalog_name, 0, 0, input)
    
    def get_catalog_stats(self, catalog_name):
        if self.per_catalog_stats.get(catalog_name) is None:
            self.per_catalog_stats[catalog_name] = {
                'total_physical_input': 0,
                'total_physical_input_read_time': 0,
                'total_input': 0
            }
        return self.per_catalog_stats.get(catalog_name)
    
    def inc_catalog_stats(self, catalog_name, physical_input, physical_input_read_time, input):
        catalog_stats = self.get_catalog_stats(catalog_name)
        catalog_stats['total_physical_input'] += physical_input
        catalog_stats['total_physical_input_read_time'] += physical_input_read_time
        catalog_stats['total_input'] += input
        self.per_catalog_stats[catalog_name] = catalog_stats
