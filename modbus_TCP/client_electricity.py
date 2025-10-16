
import traceback
from time import sleep
import datetime
import os
from dotenv import load_dotenv
import time
import asyncio
from modbus_client_TCP import mainTCP
from modbus_client_ZLAN import mainZLAN
import electricty_handlers
import water_handlers
import sensor_handlers
import steam_handlers



load_dotenv()


DATABASE_HOST = os.getenv('DATABASE_HOST')
DATABASE_NAME = os.getenv('DATABASE_NAME')
DATABASE_USER = os.getenv('DATABASE_USER')
DATABASE_PASSWORD =os.getenv('DATABASE_PASSWORD')
websocket_url=os.getenv('WS_URL')

log_file_path = os.getenv('LOG_FILE_PATH')

def log_message(message):
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"{datetime.datetime.now()}: {message}\n")

def getZlanSlaveinfo(cursor):
    try:
        # Fetch ZLAN slave information from the database
        cursor.execute("SELECT zlan_ip, number_of_meters FROM Ip_Series")
        rows = cursor.fetchall()
        zlan_slave_info = [{"slave_ip": row[0], "number_of_meters": row[1]} for row in rows]
        return zlan_slave_info
    except Exception as e:
        log_message(f"Error fetching ZLAN slave information: {traceback.format_exc()}")
        return []
    
def storeInitialData(cursor, energy_store, water_volume_store, steam_volume_store):
    electricty_handlers.getYesterdayEnergyAndCost(cursor, energy_store)
    water_handlers.getYesterdayWaterVolume(cursor, water_volume_store)
    steam_handlers.getYesterdaySteamVolume(cursor, steam_volume_store)
    zlan_slave_info = getZlanSlaveinfo(cursor)
    slave_ip_model= electricty_handlers.slaveIpAndModelMap(cursor)
    generetors= electricty_handlers.fetchGenerators(cursor)
    return zlan_slave_info,slave_ip_model,generetors




if __name__ == "__main__":

    conn = None
    client = None
    max_limit_count={}
    energy_store={}
    generetors=set()
    water_volume_store = {}
    steam_volume_store = {}
    source_data_list = []
    dgr_data_list = []
    dgr_data_15_list = []
    water_source_data_list = []
    water_dgr_data_list = []
    water_dgr_data_15_list = []
    steam_source_data_list = []
    steam_dgr_data_list = []
    steam_dgr_data_15_list = []
    sensor_data_list, sensor_dgr_data_list = [], []
    not_conn_elec = {}

    conn = electricty_handlers.connect_to_database(DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)

    while conn is None:
        conn = electricty_handlers.connect_to_database(DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
        if conn is None:
            sleep(5)

    cursor = conn.cursor()
    zlan_slave_info, slave_ip_model, generetors = storeInitialData(cursor, energy_store, water_volume_store, steam_volume_store)

    try:
        last_run_minute=datetime.datetime.now().replace(second=0, microsecond=0)
        while True:
            try:
                loop_start= time.perf_counter()
                # Attempt to establish a database connection if not already connected
                if conn is None:
                    conn = electricty_handlers.connect_to_database(DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
                    if conn is None:
                        sleep(5)
                        continue
                    cursor = conn.cursor()

                current_timestamp = datetime.datetime.now()
                isupdate= electricty_handlers.checkAnyUpdate(cursor)
                if isupdate:
                    zlan_slave_info, slave_ip_model, generetors = storeInitialData(cursor, energy_store, water_volume_store, steam_volume_store)

                #call main zlan

                dataset,dataset_storage=asyncio.run(mainZLAN(zlan_slave_info, slave_ip_model))

                results, category_dict, source_type_dict, machine_max_power_dict=electricty_handlers.fetchDataForElectricityZlan(cursor, dataset)
                slave_info, category_dict_tcp, source_type_dict_tcp, machine_max_power_dict_tcp=electricty_handlers.fetchDataForElectricityTCP(cursor)

                results_tcp=asyncio.run(mainTCP(slave_info,conn))
                results.extend(results_tcp)

                category_dict.update(category_dict_tcp)
                source_type_dict.update(source_type_dict_tcp)   
                machine_max_power_dict.update(machine_max_power_dict_tcp)


                # water data mapping
                water_results= water_handlers.fetchDataForWater(cursor, dataset)

                #sensor data mapping
                sensor_results= sensor_handlers.fetchDataForSensor(cursor, dataset)

                #steam data mapping
                steam_results= steam_handlers.fetchDataForSteam(cursor, dataset)

                # Electric data table insertion
                electricty_handlers.processReadNodeForElectricity(cursor, current_timestamp,
                                                                    results, category_dict, source_type_dict, machine_max_power_dict, energy_store,
                                                                    max_limit_count, source_data_list, dgr_data_list, dgr_data_15_list, generetors)
                electricty_handlers.bulkInsertForElectricity(cursor, source_data_list, dgr_data_list, dgr_data_15_list)


                # Water data table insertion
                water_handlers.processReadNodeForWater(cursor, current_timestamp, water_results, water_source_data_list, water_dgr_data_list,
                                                        water_dgr_data_15_list, water_volume_store)
                water_handlers.bulkInsertForWater(cursor, water_source_data_list, water_dgr_data_list, water_dgr_data_15_list)
            

                # Steam data table insertion
                steam_handlers.processReadNodeForSteam(cursor, current_timestamp, steam_results, steam_source_data_list,
                                                        steam_dgr_data_list, steam_dgr_data_15_list, steam_volume_store)
                steam_handlers.bulkInsertForSteam(cursor, steam_source_data_list, steam_dgr_data_list, steam_dgr_data_15_list)


                # Sensor data table insertion
                sensor_handlers.processReadNodeForSensor(current_timestamp, sensor_results, sensor_data_list, sensor_dgr_data_list)
                sensor_handlers.bulkInsertForSensor(cursor, sensor_data_list, sensor_dgr_data_list)


                # Process extra data

                electricty_handlers.busbarDataForElectricity(cursor,current_timestamp, source_data_list)
                electricty_handlers.allSourceData(cursor,current_timestamp)
                electricty_handlers.allLoadData(cursor,current_timestamp)

                # water extra datas
                water_handlers.busbarDataForWater(cursor, current_timestamp, water_source_data_list)
                water_handlers.allLoadData(cursor,current_timestamp)
                water_handlers.allSourceData(cursor,current_timestamp)

                # # steam extra datas
                steam_handlers.busbarDataForSteam(cursor,current_timestamp, steam_source_data_list)
                steam_handlers.allSourceData(cursor,current_timestamp)
                steam_handlers.allLoadData(cursor,current_timestamp)


                # Commit data to the database
                conn.commit()
                loop_end = time.perf_counter()
                log_message(f"Data sent to database at {current_timestamp}, Loop completed in {loop_end - loop_start:.2f} seconds.\n")
                # Pause before the next iteration
                last_run_minute=electricty_handlers.minuteChecker(last_run_minute)
                
            except Exception as e:
                # Log the error and reset connections so they reconnect in the next loop
                log_message(f"Connection Error: {traceback.format_exc()}")
                conn = None
                client = None

    finally:
        # Close Database connection if it's open
        if conn is not None:
            try:
                conn.close()
                log_message("Database connection closed.")
            except Exception as e:
                log_message(f"Error while closing database connection: {traceback.format_exc()}")


