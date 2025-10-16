class DataStore:
    def __int__(self):

        # Electricity
        self.max_limit_count={}
        self.energy_store={}
        self.generetors=set()
        self.source_data_list = []
        self.dgr_data_list = []
        self.dgr_data_15_list = []
        self.not_conn_elec = {}

        # Water
        self.water_volume_store = {}
        self.water_source_data_list = []
        self.water_dgr_data_list = []
        self.water_dgr_data_15_list = []

        # Steam
        self.steam_volume_store = {}
        self.steam_source_data_list = []
        self.steam_dgr_data_list = []
        self.steam_dgr_data_15_list = []

        # Sensor
        self.sensor_data_list= []
        self.sensor_dgr_data_list = []
    
        