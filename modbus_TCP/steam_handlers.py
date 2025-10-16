import json
import traceback
from electricty_handlers import log_message, connect_to_database
import pyodbc
import monthly_yearly_utils 
import datetime
import calendar

not_conn_steam = {}
def update_steam_volume_store(cursor, node, current_timestamp, steam_volume_store):
    """ Updates or inserts energy data ONLY at the specified times. """
    cursor.execute("SELECT COUNT(*) FROM Energy_Store WHERE node = ?", (node,))
    node_exists = cursor.fetchone()[0] > 0  

    if node_exists:
        cursor.execute('''
            UPDATE Energy_Store
            SET timedate = ?, yesterday_volume = ?
            WHERE node = ?
        ''', (steam_volume_store[node].get('last_update', current_timestamp), 
              steam_volume_store[node].get('yesterday_volume', 0.0), node))
    else:
        cursor.execute('''
            INSERT INTO Energy_Store (timedate, node, yesterday_volume)
            VALUES (?, ?, ?)
        ''', (steam_volume_store[node].get('last_update', current_timestamp), node, steam_volume_store[node].get('yesterday_volume', 0.0)))


def getYesterdaySteamVolume(cursor, steam_volume_store):
    try:
        # Fetch all node names from Source_Info
        cursor.execute("""
            SELECT node_name 
            FROM Source_Info
            WHERE resource_type = 'Steam'
        """)
        all_nodes = cursor.fetchall()  # Fetch all node names
        all_node_names = {row[0] for row in all_nodes}  # Convert to a set for easy lookup
        
        # Fetch yesterday's energy and cost data
        cursor.execute("""
            SELECT timedate, node, yesterday_volume
            FROM Energy_Store
            WHERE yesterday_volume is not NULL
        """)
        rows = cursor.fetchall()
        
        # Populate the dictionary with actual values from the second query
        for row in rows:
            timedate, node, yesterday_volume = row
            steam_volume_store[node] = {
                'last_update':timedate,
                'yesterday_volume' : yesterday_volume or 0.0,
            }
        
        # Add remaining nodes with default values if they do not appear in the second query
        for node_name in all_node_names:
            if node_name not in steam_volume_store:
                steam_volume_store[node_name] = {
                'last_update': datetime.datetime.now(),
                'yesterday_volume' : 0.0,
                }
    except pyodbc.Error as e:
        log_message(f"Database error fetching yesterday's steam volume: {traceback.format_exc()} {e}")
        raise
    except Exception as e:
        log_message(f"Error fetching yesterday's steam volume: {traceback.format_exc()} {e}")

def fetchDataForSteam(cursor, dataset):
    column_list= ['Node_Name', 'sensor_value', 'volume', 'sensor_cost', 'sensor_status']
    cursor.execute("SELECT node_name, zlan_ip, meter_no FROM Source_Info WHERE resource_type = 'Steam'  AND source_type IN ('Source', 'Load', 'Meter_Bus_Bar')")
    rows = cursor.fetchall()
    results = []

    meter_dict = {}  # Initialize the dictionary to store meter_no as key and node_name as value

    for row in rows:
        node_name, zlan_ip, meter_no = row
        temp= {}
        temp['Node_Name']=node_name
        if zlan_ip not in dataset or not isinstance(dataset[zlan_ip], list) or meter_no - 1 >= len(dataset[zlan_ip]):
            for i in range(len(column_list)):
                if column_list[i] != 'Node_Name':
                    temp[column_list[i]]=0.0
        else:
            data_for_node=dataset[zlan_ip][meter_no-1]
            for i in range(len(column_list)):
                if column_list[i] not in ['Node_Name', 'sensor_cost', 'sensor_status']:
                    temp[column_list[i]]=data_for_node[f'data_{i}']
                elif column_list[i] == 'sensor_status':
                    temp[column_list[i]]= 0 if temp['sensor_value'] == 0 else 1
                elif column_list[i] == 'sensor_cost':
                    temp[column_list[i]]= 0


        # Add meter_no and node_name to meter_dict
        meter_dict[meter_no] = node_name
        results.append(temp)
    
    return results 


def readNode(cursor, current_timestamp, temp, node_name, steam_source_data_list, steam_dgr_data_list, steam_dgr_data_15_list, steam_volume_store):

    try:
            global not_conn_steam

            if temp['volume']==0:
                temp['volume']= not_conn_steam.get(node_name, None)
                if temp['volume'] is None:
                    temp['volume'] = fetchLastData(cursor, node_name, not_conn_steam)
            else:
                not_conn_steam.pop(node_name, None)


            if steam_volume_store.get(node_name):
                yesterday_volume = steam_volume_store[node_name]['yesterday_volume']
            else:
                steam_volume_store[node_name] = {'yesterday_volume': 0}
                yesterday_volume = 0

            today_volume= temp['volume']- steam_volume_store[node_name]['yesterday_volume']
            # steam_volume_store[node_name]['today_energy'] = today_volume
            

            steam_source_data_list.append((current_timestamp, temp['Node_Name'], temp['sensor_value'], temp['volume'], temp['sensor_cost'], temp['sensor_status'], yesterday_volume, today_volume))

            steam_dgr_data_list.append((current_timestamp,temp['Node_Name'], temp['sensor_value'], temp['volume'], temp['sensor_cost'], temp['sensor_status']))

            if current_timestamp.minute % 15 == 0:
                steam_dgr_data_15_list.append((current_timestamp,temp['Node_Name'], temp['sensor_value'], temp['volume'], temp['sensor_cost'], temp['sensor_status']))



            if (current_timestamp.hour == 23 and current_timestamp.minute == 59) or (current_timestamp.hour == 0 and current_timestamp.minute <= 5):
                last_update_time = steam_volume_store[node_name].get('last_update')

                # If last_update doesn't exist or last update was NOT from previous day's 23:59
                if (current_timestamp.hour == 23 and current_timestamp.minute == 59) or (last_update_time.hour == 23 and last_update_time.minute == 0) or (last_update_time.hour != 23 and last_update_time.minute != 59):
                    # log_message(f'Midnight reset at: {current_timestamp}')
                    # Perform the reset operations
                    steam_volume_store[node_name]['yesterday_volume'] = temp['volume']
                    steam_volume_store[node_name]['last_update'] = (current_timestamp - datetime.timedelta(days=1)).replace(hour=23, minute=59, second=0, microsecond=0)
                    update_steam_volume_store(cursor, node_name, current_timestamp, steam_volume_store)



    except Exception as e:
        log_message(f"Error reading node for {node_name}: {traceback.format_exc()}")




def processReadNodeForSteam(cursor, current_timestamp, results, steam_source_data_list, steam_dgr_data_list, steam_dgr_data_15_list, steam_volume_store):
    for data in results:
        readNode(cursor, current_timestamp, data, data['Node_Name'], steam_source_data_list, steam_dgr_data_list, steam_dgr_data_15_list, steam_volume_store)



def bulkInsertForSteam(cursor, steam_source_data_list, steam_dgr_data_list, steam_dgr_data_15_list):
    try:
        
        if steam_source_data_list:
            cursor.executemany('''
                INSERT INTO Natural_Gas (timedate, node, sensor_value, volume, sensor_cost, sensor_status, yesterday_volume, today_volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', steam_source_data_list)
            # steam_source_data_list.clear()  # Clear the list after insertion

        if steam_dgr_data_list:
            cursor.executemany('''
                INSERT INTO Natural_Gas_DGR (timedate, node, sensor_value, volume, sensor_cost, sensor_status)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', steam_dgr_data_list)
            steam_dgr_data_list.clear()  # Clear the list after insertion

        if steam_dgr_data_15_list:
            cursor.executemany('''
                INSERT INTO Natural_Gas_DGR_15 (timedate, node, sensor_value, volume, sensor_cost, sensor_status)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', steam_dgr_data_15_list)
            steam_dgr_data_15_list.clear()  # Clear the list after insertion

    except Exception as e:
        log_message(f"Unexpected error during steam bulk insert: {traceback.format_exc()} {e}")
        steam_source_data_list.clear()  # Clear the list after insertion
        steam_dgr_data_list.clear()  # Clear the list after insertion    
        steam_dgr_data_15_list.clear()  # Clear the list after insertion  
        raise                                          


def steamMonthlyInsertion(cursor, current_timestamp):
    try:
        current_date = current_timestamp.date()
        # Add virtual/extra sources
        additional_nodes = ['Total_Load', 'Total_Source', 'EGB_Boiler', 'Steam_Boiler']
        sources = monthly_yearly_utils.get_gas_info(cursor)
        # Merge actual and additional nodes into one list
        all_nodes = sources + additional_nodes
        for node in all_nodes:
            sensor_value, sensor_cost, volume  = monthly_yearly_utils.get_gas_data_monthly(cursor, node, current_date)
            runtime=monthly_yearly_utils.get_gas_runtime_monthly(cursor, node, current_date)
            record_count = monthly_yearly_utils.check_existing_gas_record(cursor, current_date, node)
            
            if record_count > 0:
                monthly_yearly_utils.update_gas_record_monthly(cursor, node, sensor_value, sensor_cost, runtime, volume, current_date)
            else:
                monthly_yearly_utils.insert_gas_record_monthly(cursor, node, sensor_value, sensor_cost, runtime, volume, current_date)
    except Exception as e:
        log_message(f"Error during monthly insertion for steam: {traceback.format_exc()}")


def steamYearlyInsertion(cursor, current_timestamp):
    try:
        current_date = current_timestamp.date()
        current_month_number = current_date.month
        last_day_of_current_month = datetime.date(current_date.year, current_date.month, 
                                                calendar.monthrange(current_date.year, current_date.month)[1])
        last_entry_month = monthly_yearly_utils.get_last_entry_month_gas(cursor)
        gas_array = monthly_yearly_utils.get_gas_array(cursor, last_entry_month)
        # Add virtual/extra sources
        additional_nodes = ['Total_Load', 'Total_Source', 'EGB_Boiler', 'Steam_Boiler']
        sources = monthly_yearly_utils.get_gas_info(cursor)
        # Merge actual and additional nodes into one list
        all_nodes = sources + additional_nodes

        for source in all_nodes:
            sensor_value, sensor_cost, runtime, volume = monthly_yearly_utils.get_yearly_gas_data(cursor, source, current_month_number)
            monthly_yearly_utils.update_or_insert_gas_record(cursor, source, sensor_value, sensor_cost, runtime, volume, last_day_of_current_month, 
                                                    last_entry_month, current_month_number, gas_array)
    except Exception as e:
        log_message(f"Error during yearly insertion for Steam: {traceback.format_exc()}")


def allLoadData(cursor, current_timestamp):
    try:
        # Step 1: Fetch steam-based load nodes
        cursor.execute("""
            SELECT node_name 
            FROM Source_Info 
            WHERE Source_type = 'Load' AND resource_type = 'Steam'
        """)
        nodes = [row[0] for row in cursor.fetchall()]
        if not nodes:
            return

        # Step 2: Initialize total container
        total = {"sensor_value": 0, "sensor_cost": 0, "volume": 0, "yesterday_volume": 0, "today_volume": 0}

        # Step 3: Query latest data for these nodes
        if nodes:
            placeholders = ', '.join(['?'] * len(nodes))
            cursor.execute(f"""
                SELECT node, sensor_value, sensor_cost, volume, yesterday_volume, today_volume
                FROM Natural_Gas
                WHERE node IN ({placeholders})
                AND timedate = (SELECT MAX(timedate) FROM Water)
            """, nodes)

            for _, sensor_value, sensor_cost, volume, y_vol, t_vol in cursor.fetchall():
                total["sensor_value"] += sensor_value
                total["sensor_cost"] += sensor_cost
                total["volume"] += volume
                total["yesterday_volume"] += y_vol
                total["today_volume"] += t_vol

        # Step 4: Insert into Natural_Gas
        cursor.execute("""
            INSERT INTO Natural_Gas (timedate, node, sensor_value, sensor_cost, volume, yesterday_volume, today_volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            current_timestamp, "Total_Load",
            total["sensor_value"], total["sensor_cost"],
            total["volume"], total["yesterday_volume"], total["today_volume"]
        ))

        # Step 5: Insert into DGR
        dgr_data = (
            current_timestamp, "Total_Load",
            total["sensor_value"], total["sensor_cost"], total["volume"]
        )

        cursor.execute("""
            INSERT INTO Natural_Gas_DGR (timedate, node, sensor_value, sensor_cost, volume)
            VALUES (?, ?, ?, ?, ?)
        """, dgr_data)

        if current_timestamp.minute % 15 == 0:
            cursor.execute("""
                INSERT INTO Natural_Gas_DGR_15 (timedate, node, sensor_value, sensor_cost, volume)
                VALUES (?, ?, ?, ?, ?)
            """, dgr_data)

    except pyodbc.Error:
        log_message(f"Error Processing Natural Gas Total Load Data: {traceback.format_exc()}")
        raise
    except Exception:
        log_message(f"Unexpected error inserting Natural Gas Total Load data: {traceback.format_exc()}")



def allSourceData(cursor, current_timestamp):
    try:
        # Step 1: Get relevant source nodes
        cursor.execute("""
            SELECT node_name, category 
            FROM Source_Info 
            WHERE Source_type = 'Source' AND resource_type = 'Steam'
        """)
        rows = cursor.fetchall()
        node_per_category = {}

        for node_name, category in rows:
            node_per_category.setdefault(category, []).append(node_name)

        node_names = [node for category_nodes in node_per_category.values() for node in category_nodes]

        # Step 2: Initialize totals
        category_totals = {
            category: {"sensor_value": 0, "volume": 0, "sensor_cost": 0, "yesterday_volume": 0, "today_volume": 0}
            for category in node_per_category
        }
        category_totals["Total_Source"] = {"sensor_value": 0, "volume": 0, "sensor_cost": 0, "yesterday_volume": 0, "today_volume": 0}

        # Step 3: Aggregate sensor data if any nodes found
        if node_names:
            placeholders = ','.join(['?'] * len(node_names))
            cursor.execute(f"""
                SELECT node, sensor_value, volume, sensor_cost, yesterday_volume, today_volume 
                FROM Natural_Gas 
                WHERE node IN ({placeholders})
                  AND timedate = (SELECT MAX(timedate) FROM Natural_Gas)
            """, node_names)
            data_rows = cursor.fetchall()

            for node, sensor_value, volume, sensor_cost, y_vol, t_vol in data_rows:
                for category, nodes in node_per_category.items():
                    if node in nodes:
                        cat = category_totals[category]
                        cat["sensor_value"] += sensor_value
                        cat["volume"] += volume
                        cat["sensor_cost"] += sensor_cost
                        cat["yesterday_volume"] += y_vol
                        cat["today_volume"] += t_vol

                # Add to total
                total = category_totals["Total_Source"]
                total["sensor_value"] += sensor_value
                total["volume"] += volume
                total["sensor_cost"] += sensor_cost
                total["yesterday_volume"] += y_vol
                total["today_volume"] += t_vol

        # Step 4: Bulk insert data into tables
        steam_data = [
            (
                current_timestamp, category,
                data["sensor_value"], data["volume"], data["sensor_cost"],
                data["yesterday_volume"], data["today_volume"]
            ) for category, data in category_totals.items()
        ]

        dgr_data = [
            (
                current_timestamp, category,
                data["sensor_value"], data["volume"], data["sensor_cost"]
            ) for category, data in category_totals.items()
        ]
        if steam_data:  
            cursor.executemany("""
                INSERT INTO Natural_Gas (timedate, node, sensor_value, volume, sensor_cost, yesterday_volume, today_volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, steam_data)
        if dgr_data:
            cursor.executemany("""
                INSERT INTO Natural_Gas_DGR (timedate, node, sensor_value, volume, sensor_cost)
                VALUES (?, ?, ?, ?, ?)
            """, dgr_data)

        if current_timestamp.minute % 15 == 0 and dgr_data:
            cursor.executemany("""
                INSERT INTO Natural_Gas_DGR_15 (timedate, node, sensor_value, volume, sensor_cost)
                VALUES (?, ?, ?, ?, ?)
            """, dgr_data)

    except pyodbc.Error:
        log_message(f"Error Processing Steam Total Source Data: {traceback.format_exc()}")
        raise
    except Exception:
        log_message(f"Unexpected error inserting Steam Total Source data: {traceback.format_exc()}")



busbars = {}
def busbarDataForSteam(cursor, current_timestamp, steam_data_list):
    try:
        busbar_data_list = []
        
        global busbars
        # Step 1: Fetch all Bus_Bar and Load_Bus_Bar nodes
        cursor.execute("""
            SELECT id, node_name, source_type, connected_with, lines
            FROM Source_Info
            WHERE Source_type IN ('Bus_Bar','Load_Bus_Bar')
            AND resource_type = 'Steam'
        """)

        dataset = {item[1]: item[2] for item in steam_data_list}
        for row in cursor.fetchall():
            id, node_name, source_type, connected_with, lines_json = row

            try:
                lines = json.loads(lines_json)
            except json.JSONDecodeError:
                lines = []

            end_ids = [line["endItemId"] for line in lines]

            busbars[node_name] = {
                "id": id,
                "source_type": source_type,
                "connected_with": json.loads(connected_with) if connected_with else [],
                "end_ids": end_ids
            }
            dataset[node_name] = None


        # Step 2: Map source_info.id to (node_name, source_type)
        cursor.execute("SELECT id, node_name FROM Source_Info WHERE resource_type = 'Steam'")
        id_to_node = {row[0]: (row[1]) for row in cursor.fetchall()}

        for node_name, busbar_info in busbars.items():
            end_ids = busbar_info["end_ids"]
            source_type = busbar_info["source_type"]
            connected_with = busbar_info["connected_with"]
            flow=recursionFunction(node_name, source_type, id_to_node, dataset, connected_with, end_ids)
            busbar_data_list.append((current_timestamp, node_name, flow))


        busbars.clear()
        steam_data_list.clear()

        # pf = monthlyPfcalculation(cursor, current_timestamp, node_name, aggregates['net_energy'], aggregates['reactive_energy'], aggregates['power'])
        if busbar_data_list:
            cursor.executemany("""
                INSERT INTO Natural_Gas (timedate, node, sensor_value)
                VALUES (?, ?, ?)
            """, busbar_data_list)

        # cursor.execute("""
        #     INSERT INTO DGR_Data (timedate, node, power, cost, type, category)
        #     VALUES (?, ?, ?, ?, ?, 'Electricity')
        # """, (current_timestamp, node_name, aggregates['power'], aggregates['cost'], source_type))

        # if current_timestamp.minute % 15 == 0:
        #     cursor.execute("""
        #         INSERT INTO DGR_Data_15 (timedate, node, power, cost, type, category)
        #         VALUES (?, ?, ?, ?, ?, 'Electricity')
        #     """, (current_timestamp, node_name, aggregates['power'], aggregates['cost'], source_type))

    except pyodbc.Error:
        log_message(f"DB error inserting data for steam busbar data: {traceback.format_exc()}")
        raise

    except Exception:
        log_message(f"Critical error in busbarDataForElectricity: {traceback.format_exc()}")



def recursionFunction(node, source_type, id_to_node, dataset, connected_with, load_list):
    value=0
    if dataset[node] is not None:
        return dataset[node]
    else:
        if source_type == 'Bus_Bar':
            source_list=list(set(connected_with)-set(load_list))
            for item in source_list:
                source_name = id_to_node[item]
                if source_name in dataset and dataset[source_name] is not None:
                    value += dataset[source_name]
                else:
                    child_source_type= busbars.get(source_name, {}).get("source_type", None)
                    child_connected_with = busbars.get(source_name, {}).get("connected_with", [])
                    child_load_list = busbars.get(source_name, {}).get("end_ids", [])

                    value += recursionFunction(source_name, child_source_type, id_to_node, dataset, child_connected_with, child_load_list)
        else:
            for item in load_list:
                load_name= id_to_node[item]
                if load_name in dataset and dataset[load_name] is not None:
                    value += dataset[load_name]
                else:
                    child_source_type= busbars.get(load_name, {}).get("source_type", None)
                    child_connected_with = busbars.get(load_name, {}).get("connected_with", [])
                    child_load_list = busbars.get(load_name, {}).get("end_ids", [])

                    value += recursionFunction(load_name, child_source_type, id_to_node, dataset, child_connected_with, child_load_list)
    dataset[node] = value
    return value


def fetchLastData(cursor, node, not_conn_steam):
    try:
        cursor.execute(
            'SELECT volume FROM Natural_Gas WHERE node=? AND timedate = (SELECT MAX(timedate) FROM Natural_Gas)', 
            (node,)
        )
        row = cursor.fetchone()
        volume = row[0] if row else 0.0
        if row:
            not_conn_steam[node] = volume
        return volume

    except Exception as e:
        log_message(f'Error found in fetchLastData for steam: {traceback.format_exc()}')
        return 0.0


if __name__ == "__main__":
    pass