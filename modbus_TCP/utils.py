# class DataStore:
#     def __int__(self):

#         # Electricity
#         self.max_limit_count={}
#         self.energy_store={}
#         self.generetors=set()
#         self.source_data_list = []
#         self.dgr_data_list = []
#         self.dgr_data_15_list = []
#         self.not_conn_elec = {}

#         # Water
#         self.water_volume_store = {}
#         self.water_source_data_list = []
#         self.water_dgr_data_list = []
#         self.water_dgr_data_15_list = []

#         # Steam
#         self.steam_volume_store = {}
#         self.steam_source_data_list = []
#         self.steam_dgr_data_list = []
#         self.steam_dgr_data_15_list = []

#         # Sensor
#         self.sensor_data_list= []
#         self.sensor_dgr_data_list = []
    
        
import inspect
import os
class QueryCounterCursor:
    def __init__(self, cursor):
        self.cursor = cursor
        self.query_count = 0
        self.query_log = []  # list of {"query": str, "location": str}

    def _record_query(self, query):
        # Capture call stack info (skip first few internal frames)
        stack = inspect.stack()
        # Find the first frame outside this wrapper
        for frame_info in stack[2:]:
            if "QueryCounterCursor" not in frame_info.filename:
                filename = os.path.basename(frame_info.filename)
                function = frame_info.function
                line = frame_info.lineno
                self.query_log.append({
                    "query": str(query)[:500],  # truncate long queries
                    "location": f"{filename}:{function}:{line}"
                })
                break

    def execute(self, query, *args, **kwargs):
        self.query_count += 1
        self._record_query(query)
        return self.cursor.execute(query, *args, **kwargs)

    def executemany(self, query, *args, **kwargs):
        self.query_count += 1
        self._record_query(query)
        return self.cursor.executemany(query, *args, **kwargs)

    def fetchone(self, *args, **kwargs):
        return self.cursor.fetchone(*args, **kwargs)

    def fetchall(self, *args, **kwargs):
        return self.cursor.fetchall(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.cursor, name)

