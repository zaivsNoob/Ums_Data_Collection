
import calendar
import traceback
import websocket
from time import sleep
import pyodbc 
import datetime
import json
import os
from dotenv import load_dotenv
import time
import math
import asyncio
from modbus_client_TCP import mainTCP
from modbus_client_ZLAN import mainZLAN
import electricty_handlers
import water_handlers
import monthly_yearly_utils
import sensor_handlers
import steam_handlers



load_dotenv()


DATABASE_HOST = os.getenv('DATABASE_HOST')
DATABASE_NAME = os.getenv('DATABASE_NAME')
DATABASE_USER = os.getenv('DATABASE_USER')
DATABASE_PASSWORD =os.getenv('DATABASE_PASSWORD')
websocket_url=os.getenv('WS_URL')

log_file_path = os.getenv('LOG_FILE_PATH')

# token='02b780d3f9febcbf2dd419ed98a415e282d33f7f' 
# # websocket_url = "ws://192.168.68.20:8086/ws/live-notification/"  # Adjust as necessary
# # websocket_url = "ws://203.95.221.58:8086/ws/live-notification/"
# ws = websocket.WebSocket()
try:
    def log_message(message):
        with open(log_file_path, 'a') as log_file:
            log_file.write(f"{datetime.datetime.now()}: {message}\n")

        
    def connect_websocket(ws, websocket_url):
        """Function to connect/reconnect WebSocket."""
        try:
            if not ws.sock or not ws.sock.connected:
                ws.connect(websocket_url)
                print(f"WebSocket connected to {websocket_url}")
        except Exception as e:
            print(f"WebSocket connection failed: {traceback.format_exc()}")
            time.sleep(5)  # Wait for a few seconds before retrying

    def last_date_of_month(time):
        year = current_timestamp.year
        month = current_timestamp.month
        last_day = calendar.monthrange(year, month)[1]
        last_date_of_month = datetime.datetime(year, month, last_day)
        return last_date_of_month

    def getYesterdayEnergyAndCost(cursor, energy_store):
        try:
            # Fetch all node names from Source_Info
            cursor.execute("""
                SELECT node_name 
                FROM Source_Info
                WHERE (category IN ('Electricity', 'Grid', 'Solar', 'Diesel_Generator', 'Gas_Generator')) 
                AND source_type IN ('Source', 'Load', 'Meter_Bus_Bar', 'LB_Meter')
            """)
            all_nodes = cursor.fetchall()  # Fetch all node names
            all_node_names = {row[0] for row in all_nodes}  # Convert to a set for easy lookup
            
            # Fetch yesterday's energy and cost data
            cursor.execute("""
                SELECT timedate, node, peak_energy, off_peak_energy_1, off_peak_energy_2, generator_energy, yesterday_net_energy, today_energy
                FROM Energy_Store
                WHERE timedate = (SELECT MAX(timedate) FROM Energy_Store)
            """)
            rows = cursor.fetchall()
            
            # Populate the dictionary with actual values from the second query
            for row in rows:
                timedate, node, peak_energy, off_peak_energy_1, off_peak_energy_2, generator_energy, yesterday_net_energy, today_energy = row
                energy_store[node] = {
                    'last_update':timedate,
                    'peak_energy': peak_energy,
                    'off_peak_energy_1': off_peak_energy_1,
                    'off_peak_energy_2': off_peak_energy_2,
                    'generator_energy': generator_energy,
                    'yesterday_net_energy': yesterday_net_energy,
                    'today_energy':today_energy
                }
            
            # Add remaining nodes with default values if they do not appear in the second query
            for node_name in all_node_names:
                if node_name not in energy_store:
                    energy_store[node_name] = {
                    'last_update': datetime.datetime.now(),
                    'peak_energy': 0.0,
                    'off_peak_energy_1': 0.0,
                    'off_peak_energy_2': 0.0,
                    'generator_energy': json.dumps([]),
                    'yesterday_net_energy': 0.0,
                    'today_energy':0.0
                    }
        except Exception as e:
            log_message(f"Error fetching yesterday's energy and cost: {traceback.format_exc()}")


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



    # def send_notification(node_name):
    #     try:
    #         notification_data = {
    #             "node_name": node_name,
    #             "timedate": datetime.datetime.now().isoformat()
    #         }
    #         ws.send(json.dumps(notification_data))
    #         log_message(f"Notification sent for node: {node_name}")
    #     except Exception as e:
    #         log_message(f"Error sending notification: {traceback.format_exc()}")







    if __name__ == "__main__":

        conn = None
        client = None
        previous_status = {}
        max_limit_count={}
        energy_store={}
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
            if conn is None:
                conn = electricty_handlers.connect_to_database(DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)

                if conn is None:
                    sleep(5)
                    continue
                cursor = conn.cursor()

        if conn:
            cursor = conn.cursor()
            # Call the function to get yesterday's energy and cost
            getYesterdayEnergyAndCost(cursor, energy_store)
            water_handlers.getYesterdayWaterVolume(cursor, water_volume_store)
            steam_handlers.getYesterdaySteamVolume(cursor, steam_volume_store)
            zlan_slave_info = getZlanSlaveinfo(cursor)
            print("Signal")
        
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
                        getYesterdayEnergyAndCost(cursor, energy_store)
                        water_handlers.getYesterdayWaterVolume(cursor, water_volume_store)
                        steam_handlers.getYesterdaySteamVolume(cursor, steam_volume_store)
                        zlan_slave_info = getZlanSlaveinfo(cursor) 


                    #call main zlan

                    dataset,dataset_storage=asyncio.run(mainZLAN(zlan_slave_info))


                    results, category_dict, source_type_dict, machine_max_power_dict=electricty_handlers.fetchDataForElectricityZlan(cursor, dataset)
                    slave_info, category_dict_tcp, source_type_dict_tcp, machine_max_power_dict_tcp=electricty_handlers.fetchDataForElectricityTCP(cursor)

                    results_tcp=asyncio.run(mainTCP(slave_info,conn))
                    results.extend(results_tcp)

                    category_dict.update(category_dict_tcp)
                    source_type_dict.update(source_type_dict_tcp)   
                    machine_max_power_dict.update(machine_max_power_dict_tcp)



                    # print(results)

                    # water data mapping
                    water_results= water_handlers.fetchDataForWater(cursor, dataset)

                    #sensor data mapping
                    sensor_results= sensor_handlers.fetchDataForSensor(cursor, dataset)

                    #steam data mapping
                    steam_results= steam_handlers.fetchDataForSteam(cursor, dataset)


                    # Electric data table insertion
                    electricty_handlers.processReadNodeForElectricity(cursor, current_timestamp, results, category_dict, source_type_dict, machine_max_power_dict, energy_store, previous_status, max_limit_count, source_data_list, dgr_data_list, dgr_data_15_list)
                    electricty_handlers.bulkInsertForElectricity(cursor, source_data_list, dgr_data_list, dgr_data_15_list, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)


                    # Water data table insertion
                    water_handlers.processReadNodeForWater(cursor, current_timestamp, water_results, water_source_data_list, water_dgr_data_list, water_dgr_data_15_list, water_volume_store)
                    water_handlers.bulkInsertForWater(cursor, water_source_data_list, water_dgr_data_list, water_dgr_data_15_list, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
                

                    # Steam data table insertion
                    steam_handlers.processReadNodeForSteam(cursor, current_timestamp, steam_results, steam_source_data_list, steam_dgr_data_list, steam_dgr_data_15_list, steam_volume_store)
                    steam_handlers.bulkInsertForSteam(cursor, steam_source_data_list, steam_dgr_data_list, steam_dgr_data_15_list, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)


                    # Sensor data table insertion
                    sensor_handlers.processReadNodeForSensor(current_timestamp, sensor_results, sensor_data_list, sensor_dgr_data_list)
                    sensor_handlers.bulkInsertForSensor(cursor, sensor_data_list, sensor_dgr_data_list, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)


                    try:
                        # Process extra data

                        electricty_handlers.busbarDataForElectricity(cursor,current_timestamp, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD, source_data_list)
                        # electricty_handlers.superBusbarDataForElectricity(cursor,current_timestamp, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
                        electricty_handlers.allSourceData(cursor,current_timestamp, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
                        electricty_handlers.allLoadData(cursor,current_timestamp, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
                        electricty_handlers.monthlyPfTableInsert(cursor)


                        # water extra datas
                        water_handlers.busbarDataForWater(cursor, current_timestamp, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD, water_source_data_list)
                        water_handlers.allLoadData(cursor,current_timestamp, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
                        water_handlers.allSourceData(cursor,current_timestamp, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)

                        # # steam extra datas
                        steam_handlers.busbarDataForSteam(cursor,current_timestamp, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD, steam_source_data_list)
                        steam_handlers.allSourceData(cursor,current_timestamp, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
                        steam_handlers.allLoadData(cursor,current_timestamp, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)




                        # # All Table Monthly Yeary data
                        # electricty_handlers.electricityMonthlyInsertion(cursor, current_timestamp)
                        # electricty_handlers.electrticityYearlyInsertion(cursor, current_timestamp)
                        # water_handlers.waterMonthlyInsertion(cursor, current_timestamp)
                        # water_handlers.waterYearlyInsertion(cursor, current_timestamp)
                        # steam_handlers.steamMonthlyInsertion(cursor, current_timestamp)
                        # steam_handlers.steamYearlyInsertion(cursor, current_timestamp)



                    except Exception as e:
                        log_message(f"Error processing data: {traceback.format_exc()}")

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


except Exception as e:
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"Error : {traceback.format_exc()}\n")
