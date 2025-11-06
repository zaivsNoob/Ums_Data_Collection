import json
import traceback
from electricty_handlers import log_message, connect_to_database
import pyodbc
import monthly_yearly_utils 
import datetime
import calendar

not_conn_water = {}
def update_water_volume_store(cursor, node, current_timestamp, water_volume_store):
    """ Updates or inserts energy data ONLY at the specified times. """
    cursor.execute("SELECT COUNT(*) FROM Energy_Store WHERE node = ?", (node,))
    node_exists = cursor.fetchone()[0] > 0  

    if node_exists:
        cursor.execute('''
            UPDATE Energy_Store
            SET timedate = ?, yesterday_volume = ?
            WHERE node = ?
        ''', (water_volume_store[node].get('last_update', current_timestamp), 
              water_volume_store[node].get('yesterday_volume', 0.0), node))
    else:
        cursor.execute('''
            INSERT INTO Energy_Store (timedate, node, yesterday_volume)
            VALUES (?, ?, ?)
        ''', (water_volume_store[node].get('last_update', current_timestamp), node, water_volume_store[node].get('yesterday_volume', 0.0)))

def fetchDataForWater(cursor, dataset):
    column_list= ['Node_Name', 'sensor_value', 'volume', 'sensor_cost', 'sensor_status']
    cursor.execute("SELECT node_name, zlan_ip, meter_no, color FROM Source_Info WHERE (category IN ('Water', 'WTP', 'Sub_Mersible')) AND source_type IN ('Source', 'Load')")
    rows = cursor.fetchall()
    results = []

    meter_dict = {}  # Initialize the dictionary to store meter_no as key and node_name as value

    for row in rows:
        node_name, zlan_ip, meter_no, color = row
        temp= {}
        temp['Node_Name']=node_name
        if zlan_ip not in dataset or not isinstance(dataset[zlan_ip], list) or meter_no - 1 >= len(dataset[zlan_ip]):
            temp['sensor_value'] = 0.0
            temp['sensor_cost'] = 0.0
            temp['volume'] = 0.0
            temp['sensor_status'] = 0
            temp['color']= color
        else:
            data_for_node=dataset[zlan_ip][meter_no-1]
            for i in range(len(column_list)):
                if column_list[i] not in ['Node_Name', 'sensor_cost', 'sensor_status']:
                    temp[column_list[i]]=data_for_node.get(f'data_{i}', 0.0)
                elif column_list[i] == 'sensor_status':
                    temp[column_list[i]]= 0 if temp['sensor_value'] == 0 else 1
                elif column_list[i] == 'sensor_cost':
                    temp[column_list[i]]= 0
            temp['color']=color

        # Add meter_no and node_name to meter_dict
        meter_dict[meter_no] = node_name
        results.append(temp)
    
    return results 


def readNode(cursor, current_timestamp, temp, node_name, water_source_data_list, water_dgr_data_list, water_dgr_data_15_list, water_volume_store, meter_data):

    try:
            global not_conn_water
            if temp['volume']<=0:
                temp['volume']= not_conn_water.get(node_name, None)
                if temp['volume'] is None:
                    temp['volume'] = fetchLastData(cursor, node_name, not_conn_water)
            else:
                not_conn_water.pop(node_name, None)

            if water_volume_store.get(node_name):
                yesterday_volume = water_volume_store[node_name]['yesterday_volume']
            else:
                water_volume_store[node_name] = {'yesterday_volume': 0}
                yesterday_volume = 0
            color= temp.get('color', 'blue')
            today_volume= temp['volume']- water_volume_store[node_name]['yesterday_volume']
            cost= temp.get('sensor_cost', 0.0)
            status= temp.get('sensor_status', 0)
            volume = temp['volume']
            flow = temp['sensor_value']
        # water_volume_store[node_name]['today_energy'] = today_volume
            

            water_source_data_list.append((current_timestamp, temp['Node_Name'], flow, volume, cost, status, yesterday_volume, today_volume, color))

            water_dgr_data_list.append((current_timestamp,temp['Node_Name'], flow, volume, cost, status))

            if current_timestamp.minute % 15 == 0:
                water_dgr_data_15_list.append((current_timestamp,temp['Node_Name'], flow, volume, cost, status))



            if (current_timestamp.hour == 23 and current_timestamp.minute == 59) or (current_timestamp.hour == 0 and current_timestamp.minute <= 5):
                last_update_time = water_volume_store[node_name].get('last_update')

                # If last_update doesn't exist or last update was NOT from previous day's 23:59
                if (current_timestamp.hour == 23 and current_timestamp.minute == 59) or (last_update_time.hour == 23 and last_update_time.minute == 0) or (last_update_time.hour != 23 and last_update_time.minute != 59):
                    # log_message(f'Midnight reset at: {current_timestamp}')
                    # Perform the reset operations
                    water_volume_store[node_name]['yesterday_volume'] = volume
                    water_volume_store[node_name]['last_update'] = (current_timestamp - datetime.timedelta(days=1)).replace(hour=23, minute=59, second=0, microsecond=0)
                    update_water_volume_store(cursor, node_name, current_timestamp, water_volume_store)



    except Exception as e:
        log_message(f"Error reading node for {node_name}: {traceback.format_exc()}")

def getYesterdayWaterVolume(cursor, water_volume_store):
    try:
        # Fetch all node names from Source_Info
        cursor.execute("""
            SELECT node_name 
            FROM Source_Info
            WHERE resource_type = 'Water'
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
            water_volume_store[node] = {
                'last_update':timedate,
                'yesterday_volume' : yesterday_volume or 0.0,
            }
        
        # Add remaining nodes with default values if they do not appear in the second query
        for node_name in all_node_names:
            if node_name not in water_volume_store:
                water_volume_store[node_name] = {
                'last_update': datetime.datetime.now(),
                'yesterday_volume' : 0.0,
                }
    except pyodbc.Error as e:
        log_message(f"Database error fetching yesterday's water volume: {traceback.format_exc()} {e}")
        raise
    except Exception as e:
        log_message(f"Error fetching yesterday's water volume: {traceback.format_exc()} {e}")

def manualDenimData(cursor, water_volume_store, meter_data):
    try:
        global not_conn_water
        node_name = 'Denim'

        # Direct access for "Textile Use" and "Production Floor"
        textile_flow = meter_data.get('Textile Use', {}).get('sensor_value', 0)
        textile_volume = meter_data.get('Textile Use', {}).get('volume', 0)
        floor_flow = meter_data.get('Production Floor', {}).get('sensor_value', 0)
        floor_volume = meter_data.get('Production Floor', {}).get('volume', 0)
        color = meter_data.get('Denim', {}).get('color', 'blue')

        actual_meter_flow = textile_flow + floor_flow
        actual_meter_volume = textile_volume + floor_volume

        # Direct access for "WTP-01" and "WTP-02"
        wtp1_flow = meter_data.get('WTP-01', {}).get('sensor_value', 0)
        wtp1_volume = meter_data.get('WTP-01', {}).get('volume', 0)
        wtp2_flow = meter_data.get('WTP-02', {}).get('sensor_value', 0)
        wtp2_volume = meter_data.get('WTP-02', {}).get('volume', 0)

        soft_water_flow = wtp1_flow + wtp2_flow
        soft_water_volume = wtp1_volume + wtp2_volume

        flow = soft_water_flow - actual_meter_flow
        volume = soft_water_volume - actual_meter_volume
        cost = 0.0
        status = 1 if flow!= 0 else 0

        if volume == 0:
            volume = not_conn_water.get(node_name)
            if volume is None:
                volume = fetchLastData(cursor, node_name, not_conn_water)
        else:
            not_conn_water.pop(node_name, None)

        # Get yesterday volumes
        node_store = water_volume_store.setdefault(node_name, {})

        yesterday_volume = node_store.get('yesterday_volume', 0)

        today_volume = volume - yesterday_volume
        return volume, abs(flow), today_volume, yesterday_volume, color, cost, status


    except Exception as e:
        log_message(f"Error processing Denim and Soft Tank data: {traceback.format_exc()} for node {node_name}")



def manualRawWaterData(cursor, current_timestamp, water_source_data_list, water_dgr_data_list, water_dgr_data_15_list, water_volume_store, meter_data):
    try:
        global not_conn_water
        node_name = 'Raw Water Tank'

        # Direct access instead of summing in loop
        pump1_flow = meter_data.get('Submersible  Pump-01  (RW)', {}).get('sensor_value', 0)
        pump2_flow = meter_data.get('Submersible  Pump-02  (RW)', {}).get('sensor_value', 0)
        regen_flow = meter_data.get('Regeneration Back', {}).get('sensor_value', 0)

        pump1_volume = meter_data.get('Submersible  Pump-01  (RW)', {}).get('volume', 0)
        pump2_volume = meter_data.get('Submersible  Pump-02  (RW)', {}).get('volume', 0)
        regen_volume = meter_data.get('Regeneration Back', {}).get('volume', 0)

        flow = pump1_flow + pump2_flow + regen_flow
        volume = pump1_volume + pump2_volume + regen_volume

        cost = 0.0
        status = 1 if flow != 0 else 0

        # Fallback if volume is missing
        if volume == 0:
            volume = not_conn_water.get(node_name)
            if volume is None:
                volume = fetchLastData(cursor, node_name, not_conn_water)
        else:
            not_conn_water.pop(node_name, None)

        node_store = water_volume_store.setdefault(node_name, {})
        yesterday_volume = node_store.get('yesterday_volume', 0)
        today_volume = volume - yesterday_volume

        # Data insertion
        water_source_data_list.append((current_timestamp, node_name, flow, volume, cost, status, yesterday_volume, today_volume, 'Blue'))
        water_dgr_data_list.append((current_timestamp, node_name, flow, volume, cost, status))
        if current_timestamp.minute % 15 == 0:
            water_dgr_data_15_list.append((current_timestamp, node_name, flow, volume, cost, status))

        # Midnight volume reset logic
        if (current_timestamp.hour == 23 and current_timestamp.minute == 59) or (current_timestamp.hour == 0 and current_timestamp.minute <= 5):
            last_update_time = water_volume_store[node_name].get('last_update')

            # If last_update doesn't exist or last update was NOT from previous day's 23:59
            if (current_timestamp.hour == 23 and current_timestamp.minute == 59) or (last_update_time.hour == 23 and last_update_time.minute == 0) or (last_update_time.hour != 23 and last_update_time.minute != 59):
                # log_message(f'Midnight reset at: {current_timestamp}')
                # Perform the reset operations
                water_volume_store[node_name]['yesterday_volume'] = volume
                water_volume_store[node_name]['last_update'] = (current_timestamp - datetime.timedelta(days=1)).replace(hour=23, minute=59, second=0, microsecond=0)
                update_water_volume_store(cursor, node_name, current_timestamp, water_volume_store)

    except Exception as e:
        log_message(f"Error processing Raw Water data: {traceback.format_exc()} for node {node_name}")

def manualSoftTankData(cursor, current_timestamp, water_source_data_list, water_dgr_data_list, water_dgr_data_15_list, water_volume_store, meter_data):
    try:
        global not_conn_water
        node= 'Soft Water Tank'
        # Direct access for "WTP-01" and "WTP-02"
        wtp1_flow = meter_data.get('WTP-01', {}).get('sensor_value', 0)
        wtp1_volume = meter_data.get('WTP-01', {}).get('volume', 0)
        wtp2_flow = meter_data.get('WTP-02', {}).get('sensor_value', 0)
        wtp2_volume = meter_data.get('WTP-02', {}).get('volume', 0)

        soft_water_flow = wtp1_flow + wtp2_flow
        soft_water_volume = wtp1_volume + wtp2_volume
        soft_water_status = 1 if soft_water_flow != 0 else 0


        # Get yesterday volumes
        soft_store = water_volume_store.setdefault(node, {})

        yesterday_volume_soft = soft_store.get('yesterday_volume', 0)
        today_volume_soft = soft_water_volume - yesterday_volume_soft

        # Append entries
        water_source_data_list.append((current_timestamp, 'Soft Water Tank', soft_water_flow, soft_water_volume, 0, soft_water_status, yesterday_volume_soft, today_volume_soft, 'Blue'))
        water_dgr_data_list.append((current_timestamp, 'Soft Water Tank', soft_water_flow, soft_water_volume, 0, soft_water_status))

        if current_timestamp.minute % 15 == 0:
            water_dgr_data_15_list.append((current_timestamp, 'Soft Water Tank', soft_water_flow, soft_water_volume, 0, soft_water_status))

        # Midnight volume reset
        if (current_timestamp.hour == 23 and current_timestamp.minute == 59) or (current_timestamp.hour == 0 and current_timestamp.minute <= 5):
            last_update_time = water_volume_store['Soft Water Tank'].get('last_update')

            # If last_update doesn't exist or last update was NOT from previous day's 23:59
            if (current_timestamp.hour == 23 and current_timestamp.minute == 59) or (last_update_time.hour == 23 and last_update_time.minute == 0) or (last_update_time.hour != 23 and last_update_time.minute != 59):
                # log_message(f'Midnight reset at: {current_timestamp}')
                # Perform the reset operations
                water_volume_store['Soft Water Tank']['yesterday_volume'] = soft_water_volume
                water_volume_store['Soft Water Tank']['last_update'] = (current_timestamp - datetime.timedelta(days=1)).replace(hour=23, minute=59, second=0, microsecond=0)
                update_water_volume_store(cursor, 'Soft Water Tank', current_timestamp, water_volume_store)

    except Exception as e:
        log_message(f"Error processing Denim and Soft Tank data: {traceback.format_exc()} for node {'Soft Water Tank'}")

def manualTextileUseData(cursor, water_volume_store, meter_data):
    try:
        global not_conn_water
        node_name = 'Textile Use'

        # Get individual values
        textile_flow = meter_data.get('Textile Use', {}).get('sensor_value', 0)
        textile_volume = meter_data.get('Textile Use', {}).get('volume', 0)
        # hotwater_flow = meter_data.get('Hot Water', {}).get('sensor_value', 0)
        hotwater_volume = meter_data.get('Hot Water', {}).get('volume', 0)
        # log_message(f"Hot Water volume: {hotwater_volume}, Textile Volume: {textile_volume}")
        color = meter_data.get('Textile Use', {}).get('color', 'blue')

        # Subtract Hot Water from Textile Use
        # flow = textile_flow - hotwater_flow
        flow=textile_flow
        # log_message(f'calculated_volume_textile_use: {volume}')
        cost = 0.0
        status = 1 if flow !=0 else 0

        # Handle zero or missing volume
        if textile_volume == 0:
            textile_volume = not_conn_water.get(node_name)
            if textile_volume is None:
                textile_volume = fetchLastData(cursor, node_name, not_conn_water)
        else:
            not_conn_water.pop(node_name, None)

        textile_node_store = water_volume_store.setdefault(node_name, {})
        hotwater_node_store = water_volume_store.setdefault('Hot Water', {})
        textile_yesterday_volume = textile_node_store.get('yesterday_volume', 0)
        textile_today_volume = textile_volume - textile_yesterday_volume
        hotwater_yesterday_volume = hotwater_node_store.get('yesterday_volume', 0)
        hotwater_today_volume = hotwater_volume - hotwater_yesterday_volume
        calc_today_volume = textile_today_volume - hotwater_today_volume

        return textile_volume, flow, calc_today_volume, textile_yesterday_volume, color, cost, status

        # # Append to data lists
        # water_source_data_list.append((current_timestamp, node_name, flow, volume, cost, status, yesterday_volume, today_volume, color))
        # water_dgr_data_list.append((current_timestamp, node_name, flow, volume, cost, status))
        # if current_timestamp.minute % 15 == 0:
        #     water_dgr_data_15_list.append((current_timestamp, node_name, flow, volume, cost, status))

        # # Midnight reset
        # if (current_timestamp.hour == 23 and current_timestamp.minute == 59) or (current_timestamp.hour == 0 and current_timestamp.minute <= 5):
        #     last_update_time = water_volume_store[node_name].get('last_update')

        #     # If last_update doesn't exist or last update was NOT from previous day's 23:59
        #     if (current_timestamp.hour == 23 and current_timestamp.minute == 59) or (last_update_time.hour == 23 and last_update_time.minute == 0) or (last_update_time.hour != 23 and last_update_time.minute != 59):
        #         # log_message(f'Midnight reset at: {current_timestamp}')
        #         # Perform the reset operations
        #         water_volume_store[node_name]['yesterday_volume'] = volume
        #         water_volume_store[node_name]['last_update'] = (current_timestamp - datetime.timedelta(days=1)).replace(hour=23, minute=59, second=0, microsecond=0)
        #         update_water_volume_store(cursor, node_name, current_timestamp, water_volume_store)

    except Exception as e:
        log_message(f"Error processing Textile Use data: {traceback.format_exc()} for node {node_name}")



def processReadNodeForWater(cursor, current_timestamp, results, water_source_data_list, water_dgr_data_list, water_dgr_data_15_list, water_volume_store):

    # Precompute volumes and flows for optimization
    meter_data = {item['Node_Name']: item for item in results}

    for data in results:
        readNode(cursor, current_timestamp, data, data['Node_Name'], water_source_data_list, water_dgr_data_list, water_dgr_data_15_list, water_volume_store, meter_data)




def bulkInsertForWater(cursor, water_source_data_list, water_dgr_data_list, water_dgr_data_15_list):
    try:
        
        if water_source_data_list:
            cursor.executemany('''
                INSERT INTO Water (timedate, node, sensor_value, volume, sensor_cost, sensor_status, yesterday_volume, today_volume, color)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', water_source_data_list)
            # water_source_data_list.clear()  # Clear the list after insertion

        if water_dgr_data_list:
            cursor.executemany('''
                INSERT INTO Water_DGR (timedate, node, sensor_value, volume, sensor_cost, sensor_status)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', water_dgr_data_list)
            water_dgr_data_list.clear()  # Clear the list after insertion

        if water_dgr_data_15_list:
            cursor.executemany('''
                INSERT INTO Water_DGR_15 (timedate, node, sensor_value, volume, sensor_cost, sensor_status)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', water_dgr_data_15_list)
            water_dgr_data_15_list.clear()  # Clear the list after insertion


    except pyodbc.Error as e:
        log_message(f"Unexpected error during water bulk insert: {traceback.format_exc()} {e}")
        water_source_data_list.clear()  # Clear the list after insertion
        water_dgr_data_list.clear()  # Clear the list after insertion    
        water_dgr_data_15_list.clear()  # Clear the list after insertion 
        raise
    
    except Exception as e:
        log_message(f"Unexpected error during water bulk insert: {traceback.format_exc()} {e}")
        water_source_data_list.clear()  # Clear the list after insertion
        water_dgr_data_list.clear()  # Clear the list after insertion    
        water_dgr_data_15_list.clear()  # Clear the list after insertion 


def waterMonthlyInsertion(cursor, current_timestamp):
    try:
        current_date = current_timestamp.date()
        sources = monthly_yearly_utils.get_water_info(cursor)
        additional_nodes = ['Total_Source', 'Sub_Mersible', 'WTP', 'Total_Load']
        all_nodes = sources + additional_nodes

        if not all_nodes:
            return  # Nothing to process

        placeholders = ','.join(['?'] * len(all_nodes))
        query = f"""
            SELECT 
                node,
                COALESCE(MAX(sensor_value), 0.0) AS sensor_value,
                COALESCE(MAX(sensor_cost), 0.0) AS sensor_cost,
                COALESCE(MAX(today_volume), 0.0) AS volume,
                COUNT(DISTINCT CASE WHEN sensor_value != 0 THEN FORMAT(timedate, 'yyyy-MM-dd HH:mm') END) AS runtime
            FROM Water
            WHERE node IN ({placeholders})
            AND CAST(timedate AS DATE) = ?
            GROUP BY node
        """

        cursor.execute(query, (*all_nodes, current_date))
        results = cursor.fetchall()

        for row in results:
            node = row[0]
            sensor_value = row[1]
            sensor_cost = row[2]
            volume = row[3]
            runtime = row[4]

            record_count = monthly_yearly_utils.check_existing_water_record(cursor, current_date, node)
            if record_count > 0:
                monthly_yearly_utils.update_water_record_monthly(cursor, node, sensor_value, sensor_cost, runtime, volume, current_date)
            else:
                monthly_yearly_utils.insert_water_record_monthly(cursor, node, sensor_value, sensor_cost, runtime, volume, current_date)

    except Exception as e:
        log_message(f"Error during monthly insertion for water: {traceback.format_exc()}")



# def waterMonthlyInsertion(cursor, current_timestamp):
#     try:
#         current_date = current_timestamp.date()
#         # Add virtual/extra sources
#         sources = monthly_yearly_utils.get_water_info(cursor)
#         additional_nodes = ['Total_Source', 'Sub_Mersible', 'WTP', 'Total_Load']

#         # Merge actual and additional nodes into one list
#         all_nodes = sources + additional_nodes
#         for node in all_nodes:
#             sensor_value, sensor_cost, volume  = monthly_yearly_utils.get_water_data_monthly(cursor, node, current_date)
#             runtime=monthly_yearly_utils.get_water_runtime_monthly(cursor, node, current_date)
#             record_count = monthly_yearly_utils.check_existing_water_record(cursor, current_date, node)
            
#             if record_count > 0:
#                 monthly_yearly_utils.update_water_record_monthly(cursor, node, sensor_value, sensor_cost, runtime, volume, current_date)
#             else:
#                 monthly_yearly_utils.insert_water_record_monthly(cursor, node, sensor_value, sensor_cost, runtime, volume, current_date)
#     except Exception as e:
#         log_message(f"Error during monthly insertion for water: {traceback.format_exc()}")



def waterYearlyInsertion(cursor, current_timestamp):
    try:
        current_date = current_timestamp.date()
        current_month = current_date.month
        current_year = current_date.year
        last_day_of_month = datetime.date(current_year, current_month, calendar.monthrange(current_year, current_month)[1])

        # Get last existing entry for yearly water data
        last_entry = monthly_yearly_utils.get_last_entry_month_water(cursor)
        last_entry_month = last_entry[0].month if last_entry else None
        last_entry_year = last_entry[0].year if last_entry else None

        water_array = monthly_yearly_utils.get_water_array(cursor, last_entry)

        sources = monthly_yearly_utils.get_water_info(cursor)
        additional_nodes = ['Total_Source', 'Sub_Mersible', 'WTP', 'Total_Load']
        all_nodes = sources + additional_nodes

        placeholders = ','.join(['?'] * len(all_nodes))
        cursor.execute(f"""
            SELECT node,
                COALESCE(SUM(sensor_value), 0),
                COALESCE(SUM(sensor_cost), 0),
                COALESCE(SUM(runtime), 0),
                COALESCE(SUM(volume), 0)
            FROM Monthly_Water
            WHERE node IN ({placeholders})
              AND MONTH(date) = ?
            GROUP BY node
        """, (*all_nodes, current_month))
        yearly_data = {row[0]: row[1:] for row in cursor.fetchall()}

        for node in all_nodes:
            sensor_value, sensor_cost, runtime, volume = yearly_data.get(node, (0, 0, 0, 0))

            if node in water_array and last_entry_month == current_month and last_entry_year == current_year:
                cursor.execute("""
                    UPDATE Yearly_Water
                    SET sensor_value = ?, sensor_cost = ?, runtime = ?, volume = ?
                    WHERE MONTH(date) = ? AND YEAR(date) = ? AND node = ?
                """, (sensor_value, sensor_cost, runtime, volume, current_month, current_year, node))
            else:
                cursor.execute("""
                    INSERT INTO Yearly_Water (date, node, sensor_value, sensor_cost, runtime, volume)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (last_day_of_month, node, sensor_value, sensor_cost, runtime, volume))

    except Exception as e:
        log_message(f"Error during yearly insertion for water: {traceback.format_exc()}")





def allLoadData(cursor, current_timestamp):
    try:
        # Fetch load nodes of type Water
        cursor.execute("""
            SELECT node_name 
            FROM Source_Info 
            WHERE Source_type = 'Load' 
              AND resource_type = 'Water'
        """)
        node_names = [row[0] for row in cursor.fetchall()]
        if not node_names:
            return

        # Initialize totals
        totals = {
            "sensor_value": 0,
            "sensor_cost": 0,
            "volume": 0,
            "yesterday_volume": 0,
            "today_volume": 0
        }

        if node_names:
            placeholders = ','.join('?' for _ in node_names)
            query = f"""
                SELECT node, sensor_value, sensor_cost, volume, yesterday_volume, today_volume
                FROM Water
                WHERE node IN ({placeholders})
                AND timedate = (SELECT MAX(timedate) FROM Water)
            """
            cursor.execute(query, node_names)
            data_rows = cursor.fetchall()

            for _, sensor_value, sensor_cost, volume, y_vol, t_vol in data_rows:
                totals["sensor_value"] += sensor_value
                totals["sensor_cost"] += sensor_cost
                totals["volume"] += volume
                totals["yesterday_volume"] += y_vol
                totals["today_volume"] += t_vol

        # Prepare insert tuples
        main_insert = (
            current_timestamp, "Total_Load",
            totals["sensor_value"], totals["sensor_cost"],
            totals["volume"], totals["yesterday_volume"],
            totals["today_volume"]
        )
        dgr_insert = (
            current_timestamp, "Total_Load",
            totals["sensor_value"], totals["sensor_cost"],
            totals["volume"]
        )

        # Execute Water main table insert
        cursor.execute("""
            INSERT INTO Water (timedate, node, sensor_value, sensor_cost, volume, yesterday_volume, today_volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, main_insert)

        # Insert into Water_DGR
        cursor.execute("""
            INSERT INTO Water_DGR (timedate, node, sensor_value, sensor_cost, volume)
            VALUES (?, ?, ?, ?, ?)
        """, dgr_insert)

        # Every 15-minute interval insert
        if current_timestamp.minute % 15 == 0:
            cursor.execute("""
                INSERT INTO Water_DGR_15 (timedate, node, sensor_value, sensor_cost, volume)
                VALUES (?, ?, ?, ?, ?)
            """, dgr_insert)

    except pyodbc.Error:
        log_message(f"Error Processing Water Total Load Data: {traceback.format_exc()}")
        raise
    except Exception:
        log_message(f"Unexpected error inserting Water Total Load data: {traceback.format_exc()}")






def allSourceData(cursor, current_timestamp):
    try:
        # Step 1: Get relevant source nodes
        cursor.execute("""
            SELECT node_name, category 
            FROM Source_Info 
            WHERE Source_type = 'Source' AND category IN ('WTP', 'Sub_Mersible')
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
                FROM Water 
                WHERE node IN ({placeholders})
                  AND timedate = (SELECT MAX(timedate) FROM Water)
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
        water_data = [
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

        if water_data:
            cursor.executemany("""
                INSERT INTO Water (timedate, node, sensor_value, volume, sensor_cost, yesterday_volume, today_volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, water_data)

        if dgr_data:
            cursor.executemany("""
                INSERT INTO Water_DGR (timedate, node, sensor_value, volume, sensor_cost)
                VALUES (?, ?, ?, ?, ?)
            """, dgr_data)

        if current_timestamp.minute % 15 == 0 and dgr_data:
            cursor.executemany("""
                INSERT INTO Water_DGR_15 (timedate, node, sensor_value, volume, sensor_cost)
                VALUES (?, ?, ?, ?, ?)
            """, dgr_data)

    except pyodbc.Error:
        log_message(f"Error Processing Water Total Source Data: {traceback.format_exc()}")
        raise
    except Exception:
        log_message(f"Unexpected error inserting Water Total Source data: {traceback.format_exc()}")



# def busbarData(cursor,current_timestamp, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD):
#     try:
#         global generator_is_running
#         cursor.execute("SELECT node_name, source_type, connected_with FROM Source_Info WHERE Source_type ='Bus_Bar' AND resource_type= 'Water'")
#         rows = cursor.fetchall()
#         for row in rows:
#             node_name, source_type, connected_with = row

#             sensor_value = 0.0
#             sensor_cost=0.0
#             volume=0.0
#             today_volume=0.0
#             yesterday_volume=0.0
#             connected_with = json.loads(connected_with) if connected_with else []
            
#             if source_type == 'Bus_Bar':
#                 for item in connected_with:
#                     try:
#                         cursor.execute("SELECT node_name FROM Source_Info WHERE id=? AND Source_type='Source'", (item,))
#                         source_row = cursor.fetchone()
#                         if source_row is not None:
#                             source = source_row[0]
#                             cursor.execute("SELECT TOP 1 sensor_value, sensor_cost, volume, today_volume, yesterday_volume FROM Water WHERE node = ? ORDER BY timedate DESC", (source,))
#                             data_row = cursor.fetchone()
#                             if data_row is not None:
#                                 sensor_value += data_row[0] if data_row[0] is not None else 0.0
#                                 sensor_cost += data_row[1] if data_row[1] is not None else 0.0
#                                 volume += data_row[2] if data_row[2] is not None else 0.0
#                                 today_volume += data_row[3] if data_row[3] is not None else 0.0
#                                 yesterday_volume += data_row[4] if data_row[4] is not None else 0.0 
#                     except pyodbc.Error as e:
#                         log_message(f"Database error processing connected source: {traceback.format_exc()}")
#                         conn = connect_to_database(DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)  # Reconnect to the database
#                         cursor = conn.cursor() if conn else None
#                         continue
#                     except Exception as e:
#                         log_message(f"Error processing connected source: {traceback.format_exc()}")
#                         continue

#             # Insert into database
#             try:
                


#                 cursor.execute('''
#                     INSERT INTO Water (timedate, node, sensor_value, sensor_cost, yesterday_volume, today_volume, volume)
#                     VALUES (?, ?, ?, ?, ?, ?, ?)
#                 ''', (current_timestamp, node_name, sensor_value, sensor_cost, yesterday_volume, today_volume, volume))

                
#                 cursor.execute('''
#                     INSERT INTO Water_DGR(timedate, node, sensor_value, sensor_cost, volume)
#                     VALUES (?, ?, ?, ?, ?)
#                 ''', (current_timestamp, node_name, sensor_value, sensor_cost, volume))

#                 if current_timestamp.minute%15 == 0: 

#                     cursor.execute('''
#                         INSERT INTO Water_DGR_15(timedate, node, sensor_value, sensor_cost, volume)
#                         VALUES (?, ?, ?, ?, ?)
#                     ''', (current_timestamp, node_name, sensor_value, sensor_cost, volume))                      
#                 # conn.commit()



#             except pyodbc.Error as e:
#                 log_message(f"Database error inserting water busbar data for {node_name}: {traceback.format_exc()}")
#                 conn = connect_to_database()  # Attempt to reconnect
#                 cursor = conn.cursor() if conn else None
#             except Exception as e:
#                 log_message(f"Unexpected error inserting water busbar data: {traceback.format_exc()}")

#     except Exception as e:
#         log_message(f"Error processing busbar data: {traceback.format_exc()}")

# def busbarData(cursor, current_timestamp, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD):
#     try:
#         # Step 1: Fetch only Bus_Bar and Load_Bus_Bar (Super_Bus_Bar handled separately)
#         cursor.execute("""
#             SELECT node_name, source_type, connected_with
#             FROM Source_Info
#             WHERE resource_type = 'Water' AND source_type IN ('Bus_Bar', 'Load_Bus_Bar')
#         """)
#         busbar_rows = cursor.fetchall()

#         # Step 2: Map ID -> (node_name, source_type)
#         cursor.execute("SELECT id, node_name, source_type FROM Source_Info")
#         id_to_node_type = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

#         # Step 3: Fetch latest water data
#         cursor.execute("""
#             SELECT node, sensor_value, sensor_cost, volume, today_volume, yesterday_volume
#             FROM (
#                 SELECT *, ROW_NUMBER() OVER (PARTITION BY node ORDER BY timedate DESC) AS rn
#                 FROM Water
#             ) ranked
#             WHERE rn = 1
#         """)
#         latest_water_data = {
#             row[0]: {
#                 'sensor_value': row[1] or 0.0,
#                 'sensor_cost': row[2] or 0.0,
#                 'volume': row[3] or 0.0,
#                 'today_volume': row[4] or 0.0,
#                 'yesterday_volume': row[5] or 0.0
#             }
#             for row in cursor.fetchall()
#         }

#         # Step 4: Process each node
#         for node_name, source_type, connected_with in busbar_rows:
#             if node_name in ['Raw Water Tank', 'Soft Water Tank']:
#                 continue
#             connected_ids = json.loads(connected_with or '[]')

#             sensor_value = sensor_cost = volume = today_volume = yesterday_volume = 0.0

#             for item_id in connected_ids:
#                 node_info = id_to_node_type.get(item_id)
#                 if not node_info:
#                     continue

#                 conn_node, conn_type = node_info
#                 valid = (
#                     (source_type == 'Bus_Bar' and conn_type == 'Source') or
#                     (source_type == 'Load_Bus_Bar' and conn_type == 'Load')
#                 )

#                 if valid:
#                     data = latest_water_data.get(conn_node)
#                     if data:
#                         sensor_value += data['sensor_value']
#                         sensor_cost += data['sensor_cost']
#                         volume += data['volume']
#                         today_volume += data['today_volume']
#                         yesterday_volume += data['yesterday_volume']

#             # Step 5: Insert into Water and Water_DGR tables
#             try:
#                 cursor.execute("""
#                     INSERT INTO Water (timedate, node, sensor_value, sensor_cost, yesterday_volume, today_volume, volume)
#                     VALUES (?, ?, ?, ?, ?, ?, ?)
#                 """, (current_timestamp, node_name, sensor_value, sensor_cost, yesterday_volume, today_volume, volume))

#                 cursor.execute("""
#                     INSERT INTO Water_DGR (timedate, node, sensor_value, sensor_cost, volume)
#                     VALUES (?, ?, ?, ?, ?)
#                 """, (current_timestamp, node_name, sensor_value, sensor_cost, volume))

#                 if current_timestamp.minute % 15 == 0:
#                     cursor.execute("""
#                         INSERT INTO Water_DGR_15 (timedate, node, sensor_value, sensor_cost, volume)
#                         VALUES (?, ?, ?, ?, ?)
#                     """, (current_timestamp, node_name, sensor_value, sensor_cost, volume))

#             except pyodbc.Error:
#                 log_message(f"DB error inserting water data for {node_name}: {traceback.format_exc()}")
#                 conn = connect_to_database(DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
#                 cursor = conn.cursor() if conn else None
#             except Exception:
#                 log_message(f"Unexpected error inserting water data: {traceback.format_exc()}")

#     except Exception:
#         log_message(f"Critical error in busbarData (Water): {traceback.format_exc()}")

busbars = {}
def busbarDataForWater(cursor, current_timestamp, water_data_list):
    try:
        busbar_data_list = []
        
        global busbars
        # Step 1: Fetch all Bus_Bar and Load_Bus_Bar nodes
        cursor.execute("""
            SELECT id, node_name, source_type, connected_with, lines
            FROM Source_Info
            WHERE Source_type IN ('Bus_Bar','Load_Bus_Bar')
            AND resource_type = 'Water'
        """)

        dataset = {item[1]: item[2] for item in water_data_list}
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
        cursor.execute("SELECT id, node_name FROM Source_Info WHERE resource_type = 'Water'")
        id_to_node = {row[0]: (row[1]) for row in cursor.fetchall()}

        for node_name, busbar_info in busbars.items():
            end_ids = busbar_info["end_ids"]
            # log_message(f'Processing busbar {node_name} with end_ids: {end_ids}')
            source_type = busbar_info["source_type"]
            connected_with = busbar_info["connected_with"]
            flow=recursionFunction(node_name, source_type, id_to_node, dataset, connected_with, end_ids)
            busbar_data_list.append((current_timestamp, node_name, flow))

        busbars.clear()
        water_data_list.clear()

        # pf = monthlyPfcalculation(cursor, current_timestamp, node_name, aggregates['net_energy'], aggregates['reactive_energy'], aggregates['power'])
        if busbar_data_list:
            cursor.executemany("""
                INSERT INTO Water (timedate, node, sensor_value)
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
        log_message(f"Critical error in busbarDataForWater: {traceback.format_exc()}")
        raise
    except Exception:
        log_message(f"Critical error in busbarDataForWater: {traceback.format_exc()}")



def recursionFunction(node, source_type, id_to_node, dataset, connected_with, load_list):
    value=0
    if node in dataset and dataset[node] is not None:
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

def superBusbarData(cursor, current_timestamp, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD):
    try:
        # Step 1: Fetch only Super_Bus_Bar nodes for water
        cursor.execute("""
            SELECT node_name, connected_with
            FROM Source_Info
            WHERE resource_type = 'Water' AND source_type = 'Super_Bus_Bar'
        """)
        super_busbars = cursor.fetchall()

        # Step 2: Map ID -> (node_name, source_type)
        cursor.execute("SELECT id, node_name, source_type FROM Source_Info")
        id_to_node_type = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

        # Step 3: Fetch latest water data
        cursor.execute("""
            SELECT node, sensor_value, sensor_cost, volume, today_volume, yesterday_volume
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY node ORDER BY timedate DESC) AS rn
                FROM Water
            ) ranked
            WHERE rn = 1
        """)
        latest_water_data = {
            row[0]: {
                'sensor_value': row[1] or 0.0,
                'sensor_cost': row[2] or 0.0,
                'volume': row[3] or 0.0,
                'today_volume': row[4] or 0.0,
                'yesterday_volume': row[5] or 0.0
            }
            for row in cursor.fetchall()
        }

        # Step 4: Process each Super_Bus_Bar node
        for node_name, connected_with in super_busbars:
            if node_name in ['Raw Water Tank', 'Soft Water Tank']:
                continue
            connected_ids = json.loads(connected_with or '[]')

            sensor_value = sensor_cost = volume = today_volume = yesterday_volume = 0.0

            for item_id in connected_ids:
                node_info = id_to_node_type.get(item_id)
                if not node_info:
                    continue

                conn_node, conn_type = node_info
                if conn_type != 'Bus_Bar':
                    continue  # Only aggregate from Bus_Bar

                data = latest_water_data.get(conn_node)
                if data:
                    sensor_value += data['sensor_value']
                    sensor_cost += data['sensor_cost']
                    volume += data['volume']
                    today_volume += data['today_volume']
                    yesterday_volume += data['yesterday_volume']

            # Step 5: Insert aggregated data
            try:
                cursor.execute("""
                    INSERT INTO Water (timedate, node, sensor_value, sensor_cost, yesterday_volume, today_volume, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (current_timestamp, node_name, sensor_value, sensor_cost, yesterday_volume, today_volume, volume))

                cursor.execute("""
                    INSERT INTO Water_DGR (timedate, node, sensor_value, sensor_cost, volume)
                    VALUES (?, ?, ?, ?, ?)
                """, (current_timestamp, node_name, sensor_value, sensor_cost, volume))

                if current_timestamp.minute % 15 == 0:
                    cursor.execute("""
                        INSERT INTO Water_DGR_15 (timedate, node, sensor_value, sensor_cost, volume)
                        VALUES (?, ?, ?, ?, ?)
                    """, (current_timestamp, node_name, sensor_value, sensor_cost, volume))

            except pyodbc.Error:
                log_message(f"DB error inserting Super_Bus_Bar water data for {node_name}: {traceback.format_exc()}")
                conn = connect_to_database(DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
                cursor = conn.cursor() if conn else None
            except Exception:
                log_message(f"Unexpected error inserting Super_Bus_Bar water data: {traceback.format_exc()}")

    except Exception:
        log_message(f"Critical error in superBusbarData (Water): {traceback.format_exc()}")




def fetchLastData(cursor, node, not_conn_water):
    try:
        cursor.execute(
            'SELECT volume FROM Water WHERE node=? AND timedate = (SELECT MAX(timedate) FROM Water)', 
            (node,)
        )
        row = cursor.fetchone()
        volume = row[0] if row else 0.0
        if row:
            not_conn_water[node] = volume
        return volume

    except Exception as e:
        log_message(f'Error found in fetchLastData for water: {traceback.format_exc()}')
        return 0.0


if __name__ == "__main__":
    pass
