import calendar
from collections import defaultdict
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
import monthly_yearly_utils




load_dotenv()

not_conn_elec={}
log_file_path = os.getenv('LOG_FILE_PATH')
generator_is_running=False

# token='02b780d3f9febcbf2dd419ed98a415e282d33f7f' 
# # websocket_url = "ws://192.168.68.20:8086/ws/live-notification/"  # Adjust as necessary
# # websocket_url = "ws://203.95.221.58:8086/ws/live-notification/"
# ws = websocket.WebSocket()
try:
    def log_message(message):
        with open(log_file_path, 'a') as log_file:
            log_file.write(f"{datetime.datetime.now()}: {message}\n")

    def connect_to_database(DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD):
        try:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={DATABASE_HOST};DATABASE={DATABASE_NAME};UID={DATABASE_USER};PWD={DATABASE_PASSWORD}"
            conn = pyodbc.connect(conn_str)
            log_message("Database connection established.")
            return conn
        except Exception as e:
            log_message(f"Database connection error: {traceback.format_exc()}")
            return None
    def connect_websocket(ws, websocket_url):
        """Function to connect/reconnect WebSocket."""
        try:
            if not ws.sock or not ws.sock.connected:
                ws.connect(websocket_url)
                print(f"WebSocket connected to {websocket_url}")
        except Exception as e:
            print(f"WebSocket connection failed: {traceback.format_exc()}")
            time.sleep(5)  # Wait for a few seconds before retrying

    def last_date_of_month(current_timestamp):
        year = current_timestamp.year
        month = current_timestamp.month
        last_day = calendar.monthrange(year, month)[1]
        last_date_of_month = datetime.datetime(year, month, last_day)
        return last_date_of_month

    # def busbarDataForElectricity(cursor, current_timestamp, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD):
    #     try:
    #         global generator_is_running

    #         # Step 1: Fetch all Bus_Bar and Load_Bus_Bar nodes
    #         cursor.execute("""
    #             SELECT id, node_name, source_type, connected_with, lines
    #             FROM Source_Info
    #             WHERE Source_type IN ('Bus_Bar', 'Load_Bus_Bar')
    #             AND resource_type = 'Electricity'
    #         """)
    #         busbars = [row for row in cursor.fetchall() if row[1] not in ['Others 1', 'Others 2', 'Others 3', 'Others 4']]
    #         print(busbars)

    #         # Step 2: Map source_info.id to (node_name, source_type)
    #         cursor.execute("SELECT id, node_name, source_type FROM Source_Info")
    #         id_to_node = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

    #         # Step 3: Get latest data per node
    #         cursor.execute("""
    #             WITH Ranked AS (
    #                 SELECT *, ROW_NUMBER() OVER (PARTITION BY node ORDER BY timedate DESC) AS rn
    #                 FROM Source_Data
    #             )
    #             SELECT node, power, power_mod, cost, net_energy, today_energy, yesterday_net_energy, reactive_energy
    #             FROM Ranked WHERE rn = 1
    #         """)
    #         latest_data = {
    #             row[0]: {
    #                 'power': row[1] or 0.0,
    #                 'power_mod': row[2] or 0.0,
    #                 'cost': row[3] or 0.0,
    #                 'net_energy': row[4] or 0.0,
    #                 'today_energy': row[5] or 0.0,
    #                 'yesterday_net_energy': row[6] or 0.0,
    #                 'reactive_energy': row[7] or 0.0
    #             }
    #             for row in cursor.fetchall()
    #         }



    #         # Step 4: Process each busbar node
    #         for node_id, node_name, source_type, connected_json in busbars:
    #             connected_ids = json.loads(connected_json or '[]')
    #             aggregates = {
    #                 'power': 0.0, 'power_mod': 0.0, 'cost': 0.0,
    #                 'net_energy': 0.0, 'today_energy': 0.0,
    #                 'yesterday_net_energy': 0.0, 'reactive_energy': 0.0
    #             }

    #             for connected_id in connected_ids:
    #                 node_info = id_to_node.get(connected_id)
    #                 if not node_info:
    #                     continue
    #                 conn_node, conn_type = node_info
    #                 valid = (source_type == 'Bus_Bar' and conn_type == 'Source') or \
    #                         (source_type == 'Load_Bus_Bar' and conn_type == 'Load')

    #                 if valid:
    #                     data = latest_data.get(conn_node)
    #                     if data:
    #                         for key in aggregates:
    #                             aggregates[key] += data[key]

    #             # Step 5: Insert aggregated data
    #             # try:
    #             #     pf = monthlyPfcalculation(cursor, current_timestamp, node_name, aggregates['net_energy'], aggregates['reactive_energy'], aggregates['power'])
    #             #     cursor.execute("""
    #             #         INSERT INTO Source_Data (timedate, node, power, voltage1, voltage2, voltage3, current1, current2, current3,
    #             #                                 frequency, power_factor, cost, power_mod, cost_mod, yesterday_net_energy, today_energy,
    #             #                                 net_energy, reactive_energy)
    #             #         VALUES (?, ?, ?, 0, 0, 0, 0, 0, 0, 0, ?, ?, ?, 0, ?, ?, ?, ?)
    #             #     """, (current_timestamp, node_name, aggregates['power'], pf, aggregates['cost'], aggregates['power_mod'],
    #             #         aggregates['yesterday_net_energy'], aggregates['today_energy'], aggregates['net_energy'], aggregates['reactive_energy']))

    #             #     cursor.execute("""
    #             #         INSERT INTO DGR_Data (timedate, node, power, cost, type, category)
    #             #         VALUES (?, ?, ?, ?, ?, 'Electricity')
    #             #     """, (current_timestamp, node_name, aggregates['power'], aggregates['cost'], source_type))

    #             #     if current_timestamp.minute % 15 == 0:
    #             #         cursor.execute("""
    #             #             INSERT INTO DGR_Data_15 (timedate, node, power, cost, type, category)
    #             #             VALUES (?, ?, ?, ?, ?, 'Electricity')
    #             #         """, (current_timestamp, node_name, aggregates['power'], aggregates['cost'], source_type))

    #             # except pyodbc.Error:
    #             #     log_message(f"DB error inserting data for {node_name}: {traceback.format_exc()}")
    #             #     conn = connect_to_database(DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
    #             #     cursor = conn.cursor() if conn else None
    #             # except Exception:
    #             #     log_message(f"Unexpected error inserting data: {traceback.format_exc()}")

    #     except Exception:
    #         log_message(f"Critical error in busbarDataForElectricity: {traceback.format_exc()}")


    busbars = {}
    def busbarDataForElectricity(cursor, current_timestamp, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD, source_data_list):
        try:
            busbar_data_list = []
            global busbars
            # Step 1: Fetch all Bus_Bar and Load_Bus_Bar nodes
            cursor.execute("""
                SELECT id, node_name, source_type, connected_with, lines
                FROM Source_Info
                WHERE Source_type IN ('Bus_Bar','Load_Bus_Bar')
                AND resource_type = 'Electricity'
            """)
            # busbars = [row for row in cursor.fetchall() if row[1] not in ['Others 1', 'Others 2', 'Others 3', 'Others 4']]
            # busbars = {}
            dataset = {item['node']: item['power'] for item in source_data_list}
            for row in cursor.fetchall():
                id, node_name, source_type, connected_with, lines_json = row

                if node_name in ['Others 1', 'Others 2', 'Others 3', 'Others 4']:
                    continue

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

            # print(dataset)

            # Step 2: Map source_info.id to (node_name, source_type)
            cursor.execute("SELECT id, node_name FROM Source_Info WHERE resource_type = 'Electricity'")
            id_to_node = {row[0]: (row[1]) for row in cursor.fetchall()}

            for node_name, busbar_info in busbars.items():
                end_ids = busbar_info["end_ids"]
                source_type = busbar_info["source_type"]
                connected_with = busbar_info["connected_with"]
                power=recursionFunction(node_name, source_type, id_to_node, dataset, connected_with, end_ids)
                busbar_data_list.append((current_timestamp, node_name, power, abs(power)))


            # log_message(dataset['Diesel Generator'])
            busbars.clear()

            try:
                # pf = monthlyPfcalculation(cursor, current_timestamp, node_name, aggregates['net_energy'], aggregates['reactive_energy'], aggregates['power'])
                cursor.executemany("""
                    INSERT INTO Source_Data (timedate, node, power, power_mod)
                    VALUES (?, ?, ?, ?)
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
                log_message(f"DB error inserting data for electric busbar data: {traceback.format_exc()}")
                conn = connect_to_database(DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
                cursor = conn.cursor() if conn else None
            except Exception:
                log_message(f"Unexpected error inserting data: {traceback.format_exc()}")

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
                
                

    def allSourceData(cursor, current_timestamp, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD):
        try:
            cursor.execute("""
                SELECT node_name, category
                FROM Source_Info
                WHERE Source_type = 'Source' AND resource_type = 'Electricity'
            """)
            rows = cursor.fetchall()

            if not rows:
                return  # No source nodes to process

            node_per_category = defaultdict(list)
            for node_name, category in rows:
                node_per_category[category].append(node_name)

            node_names_tuple = tuple(node for category_nodes in node_per_category.values() for node in category_nodes)
            placeholders = ','.join('?' for _ in node_names_tuple)

            cursor.execute(f"""
                SELECT node, power, cost, yesterday_net_energy, today_energy, net_energy
                FROM Source_Data
                WHERE node IN ({placeholders}) AND timedate = (SELECT MAX(timedate) FROM Source_Data)
            """, node_names_tuple)
            source_rows = cursor.fetchall()

            # Initialize totals
            category_totals = {
                category: {"power": 0, "cost": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0}
                for category in node_per_category
            }
            category_totals["Total_Source"] = {"power": 0, "cost": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0}

            # Aggregate
            for node, power, cost, y_net, t_energy, net in source_rows:
                # Update total
                category_totals["Total_Source"]["power"] += power
                category_totals["Total_Source"]["cost"] += cost
                category_totals["Total_Source"]["yesterday_net_energy"] += y_net
                category_totals["Total_Source"]["today_energy"] += t_energy
                category_totals["Total_Source"]["net_energy"] += net

                for category, nodes in node_per_category.items():
                    if node in nodes:
                        category_totals[category]["power"] += power
                        category_totals[category]["cost"] += cost
                        category_totals[category]["yesterday_net_energy"] += y_net
                        category_totals[category]["today_energy"] += t_energy
                        category_totals[category]["net_energy"] += net

            # Prepare data
            bulk_data_source_data = [
                (current_timestamp, category, data["power"], data["cost"], data["yesterday_net_energy"], data["today_energy"], data["net_energy"])
                for category, data in category_totals.items()
            ]
            bulk_data_dgr = [
                (current_timestamp, category, data["power"], data["cost"], 'Source', 'Electricity', data["net_energy"])
                for category, data in category_totals.items()
            ]

            # Insert into Source_Data
            insert_source_query = """
                INSERT INTO Source_Data (timedate, node, power, cost, yesterday_net_energy, today_energy, net_energy)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            cursor.executemany(insert_source_query, bulk_data_source_data)

            # Insert into DGR_Data
            insert_dgr_query = """
                INSERT INTO DGR_Data (timedate, node, power, cost, type, category, net_energy)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            cursor.executemany(insert_dgr_query, bulk_data_dgr)

            if current_timestamp.minute % 15 == 0:
                insert_dgr_15_query = """
                    INSERT INTO DGR_Data_15 (timedate, node, power, cost, type, category, net_energy)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                cursor.executemany(insert_dgr_15_query, bulk_data_dgr)

        except pyodbc.Error:
            log_message(f"Error Processing Total Source Data: {traceback.format_exc()}")
            conn = connect_to_database(DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
            cursor = conn.cursor() if conn else None
        except Exception:
            log_message(f"Unexpected error inserting Total Source data: {traceback.format_exc()}")


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


    # def send_notification(node_name):
    #     try:
    #         notification_data = {
    #             "node_name": node_name,
    #             "timedate": datetime.datetime.now().isoformat()
    #         }
    #         ws.send(json.dumps(notification_data))
    #         log_message(f"Notification sent for node: {node_name}")
    #     except Exception as e:
    #         log_message(f"Error sending notification: {e}")



    def otherCalculationForReb(cursor,current_timestamp):
        try:
            # Define the cases for "Others" with source and load nodes
            others_cases = {
                'Others 1': {
                    'source_nodes': ['TR 1','Solar D'],
                    'load_nodes': ['Weaving A', 'Rotor A', 'Ring A', 'Autocone A', 'Simplex A']
                },
                'Others 2': {
                    'source_nodes': ['TR 2', 'Solar A'],
                    'load_nodes': ['Blowroom A', 'Carding A', 'AC Plant A', 'FDP A', 'AC Plant B', 'Drawing A', 'YCP A']
                },
                'Others 3': {
                    'source_nodes': ['TR 3', 'Solar B'],
                    'load_nodes': ['Blowroom B', 'Carding B', 'AC Plant C', 'FDP B', 'Ring B']
                },
                'Others 4': {
                    'source_nodes': ['TR 4', 'Solar C'],
                    'load_nodes': ['Carding C', 'AC plant D', 'Autocone B', 'Simplex B', 'Draw Frame A', 'Compressor A']
                }
            }

            # Prepare bulk insert data
            source_data_bulk = []
            dgr_data_bulk = []
            dgr_data_15_bulk = []

            for node_name, nodes in others_cases.items():
                source_nodes = "', '".join(nodes['source_nodes'])
                load_nodes = "', '".join(nodes['load_nodes'])

                # Execute the SQL query for the current case
                cursor.execute(f"""
                    SELECT 
                        SUM(CASE WHEN node IN ('{source_nodes}') THEN power ELSE 0 END) AS source_power,
                        SUM(CASE WHEN node IN ('{load_nodes}') THEN power ELSE 0 END) AS load_power,
                        SUM(CASE WHEN node IN ('{source_nodes}') THEN cost ELSE 0 END) AS source_cost,
                        SUM(CASE WHEN node IN ('{load_nodes}') THEN cost ELSE 0 END) AS load_cost,
                        SUM(CASE WHEN node IN ('{source_nodes}') THEN today_energy ELSE 0 END) AS source_energy,
                        SUM(CASE WHEN node IN ('{load_nodes}') THEN today_energy ELSE 0 END) AS load_energy
                    FROM Source_Data 
                    WHERE timedate = (SELECT MAX(timedate) FROM Source_Data)
                """)
                row = cursor.fetchone()

                # Default to zeros if no data is retrieved
                if row:
                    source_power = row[0] or 0
                    load_power = row[1] or 0
                    source_cost = row[2] or 0
                    load_cost = row[3] or 0
                    source_energy = row[4] or 0
                    load_energy = row[5] or 0

                    # Final calculations
                    power = source_power - load_power
                    cost = source_cost - load_cost
                    energy = source_energy - load_energy
                else:
                    power, cost, energy = 0, 0, 0

                # Add data to bulk insert lists
                source_data_bulk.append((current_timestamp, node_name, power, cost, energy))
                dgr_data_bulk.append((current_timestamp, node_name, power, cost, 'Load', 'Electricity'))
                if current_timestamp.minute % 15 == 0:
                    dgr_data_15_bulk.append((current_timestamp, node_name, power, cost, 'Load', 'Electricity'))

            # Perform bulk inserts
            cursor.executemany('''
                INSERT INTO Source_Data (timedate, node, power, cost, today_energy)
                VALUES (?, ?, ?, ?, ?)
            ''', source_data_bulk)

            cursor.executemany('''
                INSERT INTO DGR_Data (timedate, node, power, cost, type, category)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', dgr_data_bulk)

            if dgr_data_15_bulk:
                cursor.executemany('''
                    INSERT INTO DGR_Data_15 (timedate, node, power, cost, type, category)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', dgr_data_15_bulk)

        except pyodbc.Error as e:
            log_message(f"Error Processing Others data: {traceback.format_exc()}")
            conn = connect_to_database()  # Attempt to reconnect
            cursor = conn.cursor() if conn else None
        except Exception as e:
            log_message(f"Unexpected error inserting others data: {traceback.format_exc()}")


    def otherCalculationForGenerator(cursor,current_timestamp):
        try:
            # Define the cases for "Others" with source and load nodes
            others_cases = {
                'Others 1': {
                    'source_nodes': ['DG 01','Solar D'],
                    'load_nodes': ['Loop-2 EM 02', 'BC M1', 'Weaving A', 'Rotor A', 'Ring A', 'Autocone A', 'Simplex A']
                },
                'Others 2': {
                    'source_nodes': ['BC M1', 'Solar A'],
                    'load_nodes': ['Loop-1 EM 01', 'Blowroom A', 'Carding A', 'AC Plant A', 'FDP A', 'AC Plant B', 'Drawing A', 'YCP A']
                },
                'Others 3': {
                    'source_nodes': ['Loop-2 EM 02', 'Solar B'],
                    'load_nodes': ['BC M2', 'Blowroom B', 'Carding B', 'AC Plant C', 'FDP B', 'Ring B']
                },
                'Others 4': {
                    'source_nodes': ['BC M2', 'Solar C', 'Loop-1 EM 01'],
                    'load_nodes': ['Carding C', 'AC plant D', 'Autocone B', 'Simplex B', 'Draw Frame A']
                }
            }

            # Prepare bulk insert data
            source_data_bulk = []
            dgr_data_bulk = []
            dgr_data_15_bulk = []

            # Current timestamp for all rows

            for node_name, nodes in others_cases.items():
                source_nodes = "', '".join(nodes['source_nodes'])
                load_nodes = "', '".join(nodes['load_nodes'])

                # Execute the SQL query for the current case
                cursor.execute(f"""
                    SELECT 
                        SUM(CASE WHEN node IN ('{source_nodes}') THEN power ELSE 0 END) AS source_power,
                        SUM(CASE WHEN node IN ('{load_nodes}') THEN power ELSE 0 END) AS load_power,
                        SUM(CASE WHEN node IN ('{source_nodes}') THEN cost ELSE 0 END) AS source_cost,
                        SUM(CASE WHEN node IN ('{load_nodes}') THEN cost ELSE 0 END) AS load_cost,
                        SUM(CASE WHEN node IN ('{source_nodes}') THEN today_energy ELSE 0 END) AS source_energy,
                        SUM(CASE WHEN node IN ('{load_nodes}') THEN today_energy ELSE 0 END) AS load_energy
                    FROM Source_Data 
                    WHERE timedate = (SELECT MAX(timedate) FROM Source_Data)
                """)
                row = cursor.fetchone()

                # Default to zeros if no data is retrieved
                if row:
                    source_power = row[0] or 0
                    load_power = row[1] or 0
                    source_cost = row[2] or 0
                    load_cost = row[3] or 0
                    source_energy = row[4] or 0
                    load_energy = row[5] or 0

                    # Final calculations
                    power = source_power - load_power
                    cost = source_cost - load_cost
                    energy = source_energy - load_energy
                else:
                    power, cost, energy = 0, 0, 0

                # Add data to bulk insert lists
                source_data_bulk.append((current_timestamp, node_name, power, cost, energy))
                dgr_data_bulk.append((current_timestamp, node_name, power, cost, 'Load', 'Electricity'))
                if current_timestamp.minute % 15 == 0:
                    dgr_data_15_bulk.append((current_timestamp, node_name, power, cost, 'Load', 'Electricity'))

            # Perform bulk inserts
            cursor.executemany('''
                INSERT INTO Source_Data (timedate, node, power, cost, today_energy)
                VALUES (?, ?, ?, ?, ?)
            ''', source_data_bulk)

            cursor.executemany('''
                INSERT INTO DGR_Data (timedate, node, power, cost, type, category)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', dgr_data_bulk)

            if dgr_data_15_bulk:
                cursor.executemany('''
                    INSERT INTO DGR_Data_15 (timedate, node, power, cost, type, category)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', dgr_data_15_bulk)

        except pyodbc.Error as e:
            log_message(f"Error Processing Others data: {traceback.format_exc()}")
            conn = connect_to_database()  # Attempt to reconnect
            cursor = conn.cursor() if conn else None
        except Exception as e:
            log_message(f"Unexpected error inserting Others data: {traceback.format_exc()}")




    def monthlyPfTableInsert(cursor):
        current_timestamp = datetime.datetime.now()  # Get the current timestamp
        last_day = last_date_of_month(current_timestamp)  # Get the last date of the month
        
        # Ensure the date is formatted properly
        last_day_str = last_day.strftime('%Y-%m-%d')
        # Extract hour and minute from the current timestamp
        current_hour = current_timestamp.hour
        current_minute = current_timestamp.minute

        # Check if today is the last day of the month
        if current_timestamp.date() == last_day.date() and (current_hour == 23 and current_minute >= 50):
            try:

                # Delete all rows in PF_Store where the date is not the last day of the month
                delete_query = f"""
                    DELETE FROM PF_Store
                """
                cursor.execute(delete_query)

                # Fetch data to be inserted
                cursor.execute(f"""
                    SELECT node, MAX(net_energy) AS max_active_energy, MAX(reactive_energy) AS max_reactive_energy 
                    FROM Source_Data
                    WHERE timedate BETWEEN '{last_day_str} 23:50' AND '{last_day_str} 23:59:59'
                    GROUP BY node
                """)

                rows = cursor.fetchall()

                # Format the data for insertion
                formatted_data = [
                    (last_day_str, row.node, row.max_active_energy, row.max_reactive_energy)
                    for row in rows
                ]

                # Insert the formatted data into PF_Store
                insert_query = """
                    INSERT INTO PF_Store (date, node, max_active_energy, max_reactive_energy)
                    VALUES (?, ?, ?, ?)
                """
                cursor.executemany(insert_query, formatted_data)
            except Exception as e:
                # In case of error, roll back the transaction
                cursor.connection.rollback()
                log_message(f"An error occurred: {traceback.format_exc()}")
        else:
            return
    
    def monthlyPfcalculation(cursor, current_timestamp, node, active_energy, reactive_energy, power):
        try:
            cursor.execute(f"""SELECT max_active_energy, max_reactive_energy FROM PF_Store WHERE node = '{node}'""")
            row = cursor.fetchone()

            if row:

                # Handle possible nulls (None) in the retrieved values
                max_active_energy = row[0] if row[0] is not None else 0
                max_reactive_energy = row[1] if row[1] is not None else 0
                
                # Adjust the values as per your logic
                max_active_energy_difference = active_energy - max_active_energy
                max_reactive_energy_difference = reactive_energy - max_reactive_energy
                
                # Power factor calculation
                if max_active_energy_difference == 0 and max_reactive_energy_difference == 0:
                    return 0  # To avoid division by zero or invalid calculation
                
                monthly_pf = max_active_energy_difference / math.sqrt((max_active_energy_difference ** 2) + (max_reactive_energy_difference ** 2))

                if monthly_pf < .96 and power !=0: 
                    
                    cursor.execute('''
                    INSERT INTO Event_History(timedate, node_name, event_name,value)
                    VALUES (?, ?, ?,?)
                ''', (current_timestamp, node,"pf_failure", monthly_pf))

                return monthly_pf
            else:
                # No data found for the node
                return 0 
        except Exception as e:
            log_message(f"Error calculating power factor for {node}: {traceback.format_exc()}")
            return 0

    def update_energy_store(cursor, node, current_timestamp, energy_store):
        """ Updates or inserts energy data ONLY at the specified times. """
        cursor.execute("SELECT COUNT(*) FROM Energy_Store WHERE node = ?", (node,))
        node_exists = cursor.fetchone()[0] > 0  

        if node_exists:
            cursor.execute('''
                UPDATE Energy_Store
                SET timedate = ?, peak_energy = ?, off_peak_energy_1 = ?, off_peak_energy_2 = ?, 
                    generator_energy = ?, yesterday_net_energy = ?, today_energy = ?
                WHERE node = ?
            ''', (energy_store[node].get('last_update', current_timestamp), energy_store[node].get('peak_energy', 0.0), 
                energy_store[node].get('off_peak_energy_1', 0.0), 
                energy_store[node].get('off_peak_energy_2', 0.0), 
                energy_store[node]['generator_energy'], 
                energy_store[node]['yesterday_net_energy'], 
                energy_store[node]['today_energy'], node))
        else:
            cursor.execute('''
                INSERT INTO Energy_Store (timedate, node, peak_energy, off_peak_energy_1, off_peak_energy_2, 
                                        generator_energy, yesterday_net_energy, today_energy)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (energy_store[node].get('last_update', current_timestamp), node, energy_store[node].get('peak_energy', 0.0), 
                energy_store[node].get('off_peak_energy_1', 0.0), 
                energy_store[node].get('off_peak_energy_2', 0.0), 
                energy_store[node]['generator_energy'], 
                energy_store[node]['yesterday_net_energy'], 
                energy_store[node]['today_energy']))
            

    def calculate_generator_energy(cursor,energy_store, node, current_timestamp, net_energy):
        try:
            """Tracks generator energy usage and calculates costs for off-peak 1, peak, and off-peak 2 periods."""
            generator_data = json.loads(energy_store[node]["generator_energy"])
            
            if generator_is_running:
                if not generator_data or generator_data[-1]["end_time"] is not None:
                    generator_data.append({"start_time": str(current_timestamp), "end_time": None, "start_net_energy": net_energy})
            else:
                if generator_data and generator_data[-1]["end_time"] is None:
                    generator_data[-1]["end_time"] = str(current_timestamp)
                    generator_data[-1]["end_net_energy"] = net_energy
                    energy_store[node]["generator_energy"] = json.dumps(generator_data)
                    update_energy_store(cursor, node, current_timestamp, energy_store)

            # Compute generator energy used in each period
            generator_energy_off_peak_1 = 0.0
            generator_energy_peak = 0.0
            generator_energy_off_peak_2 = 0.0
            generator_cost = 0.0

            for entry in generator_data:
                # current_timestamp = datetime.datetime.fromisoformat(current_timestamp)
                start_time = datetime.datetime.fromisoformat(entry["start_time"])
                end_time = datetime.datetime.fromisoformat(entry["end_time"]) if entry["end_time"] is not None else None
                temp_energy_diff =0.0
                if entry["end_time"] is not None:
                    temp_energy_diff = entry["end_net_energy"] - entry["start_net_energy"]
                    if 0 <= start_time.hour < 17 and 0 <= end_time.hour < 17:
                        generator_energy_off_peak_1 += temp_energy_diff
                    elif 17 <= start_time.hour < 23 and 17 <= end_time.hour < 23:
                        generator_energy_peak += temp_energy_diff

                    elif 0<=start_time.hour<17 and 17<=end_time.hour<23:
                        generator_energy_off_peak_1+=energy_store[node]['peak_energy']-entry['start_net_energy']
                        generator_energy_peak+= entry['end_net_energy']-energy_store[node]['peak_energy']
                    elif 17<=start_time.hour<23 and 23<=end_time.hour<0:
                        generator_energy_off_peak_2+=entry['end_net_energy']-energy_store[node]['off_peak_energy_2']
                        generator_energy_peak+= energy_store[node]['off_peak_energy_2']-entry['start_net_energy']
                    else:
                        generator_energy_off_peak_2 += temp_energy_diff

                    generator_cost += temp_energy_diff * 27  # Generator cost at 27 per unit
                else:
                    temp_energy_diff = net_energy - entry["start_net_energy"]
                    if 0 <= start_time.hour < 17 and 0 <= current_timestamp.hour < 17 :
                        generator_energy_off_peak_1 += temp_energy_diff
                    elif 17 <= start_time.hour < 23 and 17 <= current_timestamp.hour < 23 :
                        generator_energy_peak += temp_energy_diff
                    elif 0<=start_time.hour<17 and 17<=current_timestamp.hour<23:
                        generator_energy_off_peak_1+=energy_store[node]['peak_energy']-entry['start_net_energy']
                        generator_energy_peak+= net_energy-energy_store[node]['peak_energy']
                    elif 17<=start_time.hour<23 and 23<=current_timestamp.hour<0:
                        generator_energy_off_peak_2+=net_energy-energy_store[node]['off_peak_energy_2']
                        generator_energy_peak+= energy_store[node]['off_peak_energy_2']-entry['start_net_energy']
                    else:
                        generator_energy_off_peak_2 += temp_energy_diff

                    generator_cost += temp_energy_diff * 27  # Generator cost at 27 per unit

            # Save updated generator data
            energy_store[node]["generator_energy"] = json.dumps(generator_data)
            
            return generator_energy_off_peak_1, generator_energy_peak, generator_energy_off_peak_2, generator_cost
        except Exception as e:
            log_message(f"Error Found in calculate_genrator_energy {traceback.format_exc()}")

    def costCalculation(cursor, node, current_timestamp, energy_store, net_energy):
        try:
            # Check if current time matches specific update times
            if current_timestamp.hour == 0 and current_timestamp.minute ==0:
                # energy_store[node]['off_peak_energy_1'] = net_energy
                # energy_store[node]['last_update'] = current_timestamp
                if generator_is_running:
                    generator_data = json.loads(energy_store[node]["generator_energy"])
                    generator_data.append({"start_time": str(current_timestamp), "end_time": None, "start_net_energy": net_energy})
                    energy_store[node]["generator_energy"] = json.dumps(generator_data)
                # update_energy_store(cursor, node, current_timestamp, energy_store)

            elif (current_timestamp.hour == 17 and current_timestamp.minute == 0) or (current_timestamp.hour == 17 and current_timestamp.minute <= 5):
                last_update_time = energy_store[node].get('last_update')

                if (current_timestamp.hour == 17 and current_timestamp.minute == 0) or (last_update_time.hour != 17 and last_update_time.minute != 0):
                    log_message(f'peak update at: {current_timestamp}')
                    energy_store[node]['peak_energy'] = net_energy
                    energy_store[node]['last_update'] = current_timestamp
                    energy_store[node]['last_update'] = current_timestamp.replace(hour=17, minute=0, second=0, microsecond=0)
                    update_energy_store(cursor, node, current_timestamp, energy_store)

            elif (current_timestamp.hour == 23 and current_timestamp.minute == 0) or (current_timestamp.hour == 23 and current_timestamp.minute <= 5):
                last_update_time = energy_store[node].get('last_update')

                if (current_timestamp.hour == 23 and current_timestamp.minute == 0) or (last_update_time.hour != 23 and last_update_time.minute != 0):
                    log_message(f'off_peak_2 update at: {current_timestamp}')
                    energy_store[node]['off_peak_energy_2'] = net_energy
                    energy_store[node]['last_update'] = current_timestamp
                    energy_store[node]['last_update'] = current_timestamp.replace(hour=23, minute=0, second=0, microsecond=0)
                    update_energy_store(cursor, node, current_timestamp, energy_store)

            # Calculate generator energy usage
            generator_energy_off_peak_1, generator_energy_peak, generator_energy_off_peak_2, generator_cost = (
                calculate_generator_energy(cursor, energy_store, node, current_timestamp, net_energy)
            )

            # Cost Calculations
            if 0 <= current_timestamp.hour < 17:  # Off-peak 1
                cost = ((net_energy - energy_store[node]['off_peak_energy_1']- generator_energy_off_peak_1) * 9.69 
                        + generator_cost) if energy_store[node]['off_peak_energy_1'] != 0.0 else 0.0

            elif 17 <= current_timestamp.hour < 23:  # Peak
                cost = (((net_energy - energy_store[node]['peak_energy']- generator_energy_peak) * 13.47) + 
                        ((energy_store[node]['peak_energy'] - energy_store[node]['off_peak_energy_1'] - generator_energy_off_peak_1) * 9.69)) + generator_cost if energy_store[node]['peak_energy'] != 0.0 else 0.0

            else:  # Off-peak 2
                cost = (((net_energy - energy_store[node]['off_peak_energy_2']- generator_energy_off_peak_2) * 9.69 ) + 
                        ((energy_store[node]['off_peak_energy_2'] - energy_store[node]['peak_energy'] - generator_energy_peak) * 13.47) + 
                        ((energy_store[node]['peak_energy'] - energy_store[node]['off_peak_energy_1'] - generator_energy_off_peak_1) * 9.69)) + generator_cost if energy_store[node]['off_peak_energy_2'] != 0.0 else 0.0
            if node=='Solar 1':
                print(f'generator_cost:{generator_cost}, generator off_peak_energy: {generator_energy_off_peak_1}, generator peak_energy: {generator_energy_peak}, Total cost{cost}')
            return cost

        except Exception as e:
            log_message(f'Error found in the costCalculation function {traceback.format_exc()}')
            raise


    def readNode(cursor,current_timestamp,temp,node_name,category,source_type, machine_max_power, energy_store, previous_status, max_limit_count, source_data_list, dgr_data_list, dgr_data_15_list):

        try:
                global generator_is_running, not_conn_elec


                if node_name== 'DG 01':
                    if temp["Power"]>0:

                        generator_is_running=True
                    else:
                        generator_is_running=False
  
                #will be optimize later
                # for notification
                if temp['Net_Energy']==0:
                    temp['Net_Energy']= not_conn_elec.get(node_name, None)
                    if temp['Net_Energy'] is None:
                        temp['Net_Energy'] = fetchLastData(cursor, node_name, not_conn_elec)
                else:
                    not_conn_elec.pop(node_name, None)

                current_status = temp['Status']
                monthly_pf= monthlyPfcalculation(cursor, current_timestamp, node_name, temp["Net_Energy"], temp['Reactive_Energy'],temp['Power'])
                


                # if node_name in previous_status:
                # #     # Check if previous status was True and current status is False
                #     if previous_status[node_name] == True and current_status == False:
                #             # Status changed from True to False, insert into Notification table
                #             cursor.execute('''
                #                 INSERT INTO home_notification (timedate, node_name)
                #                 VALUES (?, ?)
                #             ''', (current_timestamp, node_name))
                #             # conn.commit()
                #             ws.connect(websocket_url, header=[f"Authorization: Bearer {token}"])
                #             send_notification(node_name)
                #             print(f"Notification sent: Node {node_name} is OFF")
                #             ws.close()
                            
                previous_status[node_name] = current_status

                if energy_store.get(node_name):
                    yesterday_energy = energy_store[node_name]['yesterday_net_energy']
                else:
                    energy_store[node_name] = {'yesterday_net_energy': 0}
                    yesterday_energy = 0

                today_energy= temp['Net_Energy']- energy_store[node_name]['yesterday_net_energy']
                energy_store[node_name]['today_energy'] = today_energy
                
                temp['Cost']=costCalculation(cursor, node_name, current_timestamp, energy_store, temp["Net_Energy"])




                # Handle machine_max_power logic
                if machine_max_power is None:
                    color = "#6CE759"  # Always Green if no machine_max_power is provided
                else:
                    if temp["Power"] >= machine_max_power:
                        max_limit_count[node_name] = max_limit_count.get(node_name, 0) + 1
                    else:
                        max_limit_count[node_name] = 0  # Reset count if power is within limits

                    # Check the count to decide color
                    if max_limit_count[node_name] >= 3:
                        color = "#FF7C72"
                        cursor.execute('''
                        INSERT INTO Event_History(timedate, node_name, event_name,value)
                        VALUES (?, ?, ?,?)
                    ''', (current_timestamp, node_name,"power_overload", temp['Power']))


                    else:
                        color = "#6CE759"   #Green

                source_data_list.append((current_timestamp, temp['Node_Name'], temp['Power'], temp['Voltage1'], temp['Voltage2'], temp['Voltage3'], temp['Current1'],
                                        temp['Current2'], temp['Current3'], temp['Frequency'], monthly_pf, temp['Cost'], temp['Status'], abs(temp['Power']),
                                        abs(temp['Cost']), energy_store[node_name]['yesterday_net_energy'], today_energy, temp['Net_Energy'], color, temp['Reactive_Energy']))

                dgr_data_list.append((current_timestamp,temp['Node_Name'], temp['Power'], temp['Cost'], source_type, category, temp['Net_Energy']))

                if current_timestamp.minute % 15 == 0:
                    dgr_data_15_list.append((current_timestamp,temp['Node_Name'], temp['Power'], temp['Cost'], source_type, category, temp['Net_Energy']))



                if (current_timestamp.hour == 23 and current_timestamp.minute == 59) or (current_timestamp.hour == 0 and current_timestamp.minute <= 5):
                    
                    last_update_time = energy_store[node_name].get('last_update')

                    # If last_update doesn't exist or last update was NOT from previous day's 23:59
                    if (current_timestamp.hour == 23 and current_timestamp.minute == 59) or (last_update_time.hour == 23 and last_update_time.minute == 0) or (last_update_time.hour != 23 and last_update_time.minute != 59):
                        log_message(f'Midnight reset at: {current_timestamp}')
                        # Perform the reset operations
                        energy_store[node_name]['yesterday_net_energy'] = temp['Net_Energy']
                        energy_store[node_name]['today_energy'] = 0.0
                        energy_store[node_name]['off_peak_energy_1'] = temp['Net_Energy']
                        energy_store[node_name]['peak_energy'] = 0.0
                        energy_store[node_name]['off_peak_energy_2'] = 0.0
                        energy_store[node_name]["generator_energy"] = json.dumps([])
                        energy_store[node_name]['last_update'] = (current_timestamp - datetime.timedelta(days=1)).replace(hour=23, minute=59, second=0, microsecond=0)
                        update_energy_store(cursor, node_name, current_timestamp, energy_store) #

  

        except Exception as e:
            log_message(f"Error reading node for {node_name}: {traceback.format_exc()}")

    def fetchDataForElectricityZlan(cursor, dataset):
        try:
            column_list= ['Node_Name', 'Net_Energy', 'Voltage1', 'Voltage2', 'Voltage3', 'Current1', 'Current2', 'Current3', 'Power', 'Frequency', 'Reactive_Energy', 'Status']
            cursor.execute("SELECT zlan_ip, meter_model, node_name, category, source_type, machine_max_power,meter_no FROM Source_Info WHERE (category IN ('Electricity', 'Grid', 'Solar', 'Diesel_Generator', 'Gas_Generator')) AND source_type IN ('Source', 'Load', 'Meter_Bus_Bar', 'LB_Meter') AND connection_type = 'Zlan'")
            rows = cursor.fetchall()
            results = []

            category_dict = {}
            source_type_dict = {}
            machine_max_power_dict = {}


            meter_dict = {}  # Initialize the dictionary to store meter_no as key and node_name as value

            for row in rows:
                zlan_ip, meter_model, node_name, category, source_type, machine_max_power, meter_no = row
                temp= {}
                temp['Node_Name']=node_name
                if zlan_ip not in dataset or not isinstance(dataset[zlan_ip], list) or meter_no - 1 >= len(dataset[zlan_ip]):
                    for i in range(len(column_list)):
                        if column_list[i] != 'Node_Name' and column_list[i] != 'Status':
                            temp[column_list[i]]=0.0
                        elif column_list[i] == 'Status':
                            temp[column_list[i]]= 0 if temp['Power'] == 0 else 1
                else:
                    data_for_node=dataset[zlan_ip][meter_no-1]
                    for i in range(len(column_list)):
                        if column_list[i] != 'Node_Name' and column_list[i] != 'Status':
                            temp[column_list[i]]=data_for_node[f'data_{i}']
                        elif column_list[i] == 'Status':
                            temp[column_list[i]]= 0 if temp['Power'] == 0 else 1
                # Populate the dictionaries
                category_dict[node_name] = category
                source_type_dict[node_name] = source_type
                machine_max_power_dict[node_name] = machine_max_power

                # Add meter_no and node_name to meter_dict
                meter_dict[meter_no] = node_name
                results.append(temp)
            return results, category_dict, source_type_dict, machine_max_power_dict
        except Exception as e:
            log_message(f"Error fetching data for electricity:{node_name} ,{traceback.format_exc()}")
            return [], {}, {}, {}

    def fetchDataForElectricityTCP(cursor):
        try:
            cursor.execute("SELECT ip3, ip4_unit_id, meter_model, node_name, category, source_type, machine_max_power FROM Source_Info WHERE (category NOT IN ('Water', 'Natural_Gas', 'Other_Sensor')) AND source_type IN ('Source', 'Load', 'Meter_Bus_Bar', 'LB_Meter') AND connection_type = 'TCP/IP'")
            rows = cursor.fetchall()
            slave_info = []

            category_dict = {}
            source_type_dict = {}
            machine_max_power_dict = {}


            for row in rows:
                ip3, ip4_unit_id, meter_model, node_name, category, source_type, machine_max_power = row
                

                    # Create the slave_info data structure
                slave_info.append(
                        {"slave_ip": f"192.168.{int(ip3)}.{int(ip4_unit_id)}", "meter_type": meter_model, "node_name": node_name}
                    )

                # Populate the dictionaries
                category_dict[node_name] = category
                source_type_dict[node_name] = source_type
                machine_max_power_dict[node_name] = machine_max_power

            return slave_info, category_dict, source_type_dict, machine_max_power_dict    

        except Exception as e:
            log_message(f"Error fetching data for electricity TCP: {traceback.format_exc()}")
            return []


    def processReadNodeForElectricity(cursor, current_timestamp, results, category_dict, source_type_dict, machine_max_power_dict, energy_store, previous_status, max_limit_count, source_data_list, dgr_data_list, dgr_data_15_list):
        for data in results:
            node_name = data['Node_Name']

            category = category_dict.get(node_name)
            source_type = source_type_dict.get(node_name)
            machine_max_power = machine_max_power_dict.get(node_name)

            # print(f"Received data: {data}")  # Process data as needed

            readNode(cursor, current_timestamp, data, data['Node_Name'], category, source_type, machine_max_power, energy_store, previous_status, max_limit_count, source_data_list, dgr_data_list, dgr_data_15_list)

    def bulkInsertForElectricity(cursor, source_data_list, dgr_data_list, dgr_data_15_list, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD):
        try:
            
            if source_data_list:
                cursor.executemany('''
                    INSERT INTO Source_Data (timedate, node, power, voltage1, voltage2, voltage3, current1, current2, current3, frequency, power_factor, cost, status, power_mod, cost_mod, yesterday_net_energy, today_energy, net_energy, color, reactive_energy)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', source_data_list)
                source_data_list.clear()  # Clear the list after insertion

            if dgr_data_list:
                cursor.executemany('''
                    INSERT INTO DGR_Data (timedate, node, power, cost, type, category, net_energy)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', dgr_data_list)
                dgr_data_list.clear()  # Clear the list after insertion

            if dgr_data_15_list:
                cursor.executemany('''
                    INSERT INTO DGR_Data_15 (timedate, node, power, cost, type, category, net_energy)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', dgr_data_15_list)
                dgr_data_15_list.clear()  # Clear the list after insertion
        except pyodbc.Error as e:
            log_message(f"Database error during bulk insert: {traceback.format_exc()} {source_data_list}")
            conn = connect_to_database(DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)  # Attempt to reconnect
            cursor = conn.cursor() if conn else None

            source_data_list.clear()  # Clear the list after insertion
            dgr_data_list.clear()  # Clear the list after insertion    
            dgr_data_15_list.clear()  # Clear the list after insertion                                            
        except Exception as e:
            log_message(f"Unexpected error during bulk insert: {traceback.format_exc()}")

    # def electrticityMonthlyInsertion(cursor, current_timestamp):
    #     try:
    #         current_date = current_timestamp.date()
    #         sources = monthly_yearly_utils.get_source_info(cursor)
    #         # Add virtual/extra sources
    #         additional_nodes = ['Solar', 'Grid', 'Diesel_Generator', 'Gas_Generator', 'Total_Source', 'Total_Load']

    #         # Merge actual and additional nodes into one list
    #         all_nodes = sources + additional_nodes

    #         for node in all_nodes:
    #             result = monthly_yearly_utils.get_energy_and_cost_monthly(cursor, node, current_date)

    #             if not result:
    #                 continue  # Skip if no data

    #             energy_result, cost, energy_mod, cost_mod = result
    #             record_count = monthly_yearly_utils.check_existing_record(cursor, current_date, node)
    #             runtime = monthly_yearly_utils.get_runtime_monthly(cursor, node, current_date)

    #             if record_count > 0:
    #                 monthly_yearly_utils.update_record_monthly(
    #                     cursor, energy_result, cost, runtime, energy_mod, cost_mod, current_date, node
    #                 )
    #             else:
    #                 monthly_yearly_utils.insert_record_monthly(
    #                     cursor, energy_result, cost, runtime, energy_mod, cost_mod, current_date, node
    #                 )

    #     except Exception as e:
    #         log_message("Error in electrticityMonthlyInsertion:", {traceback.format_exc()})


    def electricityMonthlyInsertion(cursor, current_timestamp):
        try:
            current_date = current_timestamp.date()
            sources = monthly_yearly_utils.get_source_info(cursor)
            additional_nodes = ['Solar', 'Grid', 'Diesel_Generator', 'Gas_Generator', 'Total_Source', 'Total_Load']
            all_nodes = sources + additional_nodes

            if not all_nodes:
                return  # Nothing to process

            placeholders = ','.join(['?'] * len(all_nodes))
            query = f"""
                SELECT 
                    node,
                    COALESCE(MAX(today_energy), 0.0) AS today_energy,
                    COALESCE(MAX(cost), 0.0) AS cost,
                    COALESCE(MAX(cost_mod), 0.0) AS cost_mod,
                    COALESCE(MAX(ABS(today_energy)), 0.0) AS energy_mod,
                    COUNT(DISTINCT CASE WHEN power > 0 THEN FORMAT(timedate, 'yyyy-MM-dd HH:mm') END) AS runtime
                FROM Source_Data
                WHERE node IN ({placeholders})
                AND CAST(timedate AS DATE) = ?
                GROUP BY node
            """

            cursor.execute(query, (*all_nodes, current_date))
            results = cursor.fetchall()

            for row in results:
                node = row[0]
                energy_result = row[1]
                cost = row[2]
                cost_mod = row[3]
                energy_mod = row[4]
                runtime = row[5]

                record_count = monthly_yearly_utils.check_existing_record(cursor, current_date, node)
                if record_count > 0:
                    monthly_yearly_utils.update_record_monthly(
                        cursor, energy_result, cost, runtime, energy_mod, cost_mod, current_date, node
                    )
                else:
                    monthly_yearly_utils.insert_record_monthly(
                        cursor, energy_result, cost, runtime, energy_mod, cost_mod, current_date, node
                    )

        except Exception as e:
            log_message("Error in electricityMonthlyInsertion: " + traceback.format_exc())

    # def electrticityYearlyInsertion(cursor, current_timestamp):
    #     try:
    #         current_date = current_timestamp.date()
    #         current_month_number = current_date.month
    #         last_day_of_current_month = datetime.date(current_date.year, current_date.month, 
    #                                                 calendar.monthrange(current_date.year, current_date.month)[1])

    #         last_entry_month = monthly_yearly_utils.get_last_entry_month(cursor)
    #         node_array = monthly_yearly_utils.get_node_array(cursor, last_entry_month)
    #         sources = monthly_yearly_utils.get_source_info(cursor)
    #         additional_nodes = ['Solar', 'Grid', 'Diesel_Generator', 'Gas_Generator', 'Total_Source', 'Total_Load']

    #         # Merge actual and additional nodes into one list
    #         all_nodes = sources + additional_nodes

    #         for node in all_nodes:
    #             energy_result, cost, powercut, count_powercut, energy_mod, cost_mod, runtime = monthly_yearly_utils.get_yearly_data(cursor, node, current_month_number)
    #             monthly_yearly_utils.update_or_insert_record(cursor, node, energy_result, cost, powercut, count_powercut, energy_mod, cost_mod, runtime, last_day_of_current_month, 
    #                                                     last_entry_month, current_month_number, node_array)
    #     except Exception as e:
    #         log_message(f"Error in electricity yearly insertion: {traceback.format_exc()}")



    def electrticityYearlyInsertion(cursor, current_timestamp):
        try:
            current_date = current_timestamp.date()
            current_month = current_date.month
            current_year = current_date.year
            last_day_of_month = datetime.date(current_year, current_month, calendar.monthrange(current_year, current_month)[1])

            last_entry = monthly_yearly_utils.get_last_entry_month(cursor)
            last_entry_month = last_entry[0].month if last_entry else None
            last_entry_year = last_entry[0].year if last_entry else None

            existing_nodes = monthly_yearly_utils.get_node_array(cursor, last_entry)
            db_sources = monthly_yearly_utils.get_source_info(cursor)
            additional_nodes = ['Solar', 'Grid', 'Diesel_Generator', 'Gas_Generator', 'Total_Source', 'Total_Load']
            all_nodes = db_sources + additional_nodes

            # Pre-fetch power cut data for all nodes
            cursor.execute(f"""
                SELECT generator_name,
                    COALESCE(SUM(duration_in_min), 0) AS total_duration,
                    COUNT(*) AS count_powercuts
                FROM Power_Cut
                WHERE MONTH(start_time) = ?
                GROUP BY generator_name
            """, (current_month,))
            powercut_data = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

            # Pre-fetch monthly energy/cost/runtime data for all nodes
            placeholders = ','.join(['?'] * len(all_nodes))
            cursor.execute(f"""
                SELECT node,
                    COALESCE(SUM(energy), 0),
                    COALESCE(SUM(cost), 0),
                    COALESCE(SUM(energy_mod), 0),
                    COALESCE(SUM(cost_mod), 0),
                    COALESCE(SUM(runtime), 0)
                FROM Monthly_Total_Energy
                WHERE node IN ({placeholders})
                AND MONTH(date) = ?
                GROUP BY node
            """, (*all_nodes, current_month))
            energy_data = {row[0]: row[1:] for row in cursor.fetchall()}

            for node in all_nodes:
                energy, cost, energy_mod, cost_mod, runtime = energy_data.get(node, (0, 0, 0, 0, 0))
                powercut, count_powercut = powercut_data.get(node, (0, 0))

                # Update or insert based on previous existence
                if node in existing_nodes and last_entry_month == current_month and last_entry_year == current_year:
                    cursor.execute("""
                        UPDATE Yearly_Total_Energy
                        SET energy = ?, cost = ?, powercut_in_min = ?, count_powercuts = ?, energy_mod = ?, cost_mod = ?, runtime = ?
                        WHERE MONTH(date) = ? AND YEAR(date) = ? AND node = ?
                    """, (energy, cost, powercut, count_powercut, energy_mod, cost_mod, runtime, current_month, current_year, node))
                else:
                    cursor.execute("""
                        INSERT INTO Yearly_Total_Energy (date, node, energy, cost, powercut_in_min, count_powercuts, energy_mod, cost_mod, runtime)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (last_day_of_month, node, energy, cost, powercut, count_powercut, energy_mod, cost_mod, runtime))

        except Exception as e:
            log_message(f"Error in electricity yearly insertion: {traceback.format_exc()}")



    # def allLoadData(cursor,current_timestamp, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD):
    #     try:

    #         cursor.execute("SELECT node_name, category FROM Source_Info WHERE Source_type = 'Load' AND (category IN ('Electricity', 'Grid', 'Solar', 'Diesel_Generator', 'Gas_Generator'))")
    #         rows = cursor.fetchall()
    #         node_names_tuple = tuple(node_name for node_name, category in rows)
            
    #         if node_names_tuple:
    #             placeholders = ','.join('?' for _ in node_names_tuple)
    #             query = f"""
    #                 SELECT node, power, cost, yesterday_net_energy, today_energy, net_energy
    #                 FROM Source_Data
    #                 WHERE node IN ({placeholders}) AND timedate = (SELECT MAX(timedate) FROM Source_Data)
    #             """
    #             cursor.execute(query, node_names_tuple)
    #             rows = cursor.fetchall()
    #         else:
    #             rows = []
    #         category_totals={}

    #         # Add total_source to category_totals
    #         category_totals["Total_Load"] = {"power": 0, "cost": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0}

    #         # Process query results
    #         for node, power, cost, yesterday_net_energy, today_energy, net_energy in rows:
    #             # Update the total_source sums
    #             category_totals["Total_Load"]["power"] += power
    #             category_totals["Total_Load"]["cost"] += cost
    #             category_totals["Total_Load"]["yesterday_net_energy"] += yesterday_net_energy
    #             category_totals["Total_Load"]["today_energy"] += today_energy
    #             category_totals["Total_Load"]["net_energy"] += net_energy

    #         # SQL query for bulk insert into Source_Data
    #         insert_query = """
    #             INSERT INTO Source_Data (timedate, node, power, cost, yesterday_net_energy, today_energy, net_energy)
    #             VALUES (?, ?, ?, ? , ?, ?, ?)
    #         """
    #         # Execute the bulk insert
    #         cursor.execute(insert_query, (current_timestamp, "Total_Load", category_totals["Total_Load"]["power"], category_totals["Total_Load"]["cost"], category_totals["Total_Load"]["yesterday_net_energy"], category_totals["Total_Load"]["today_energy"], category_totals["Total_Load"]["net_energy"]))

    #         insert_query = """
    #             INSERT INTO DGR_Data (timedate, node, power, cost, type, category, net_energy)
    #             VALUES (?, ?, ?, ?, ?, ?, ?)
    #         """
    #         # Execute the bulk insert
    #         cursor.execute(insert_query, (current_timestamp, "Total_Load", category_totals["Total_Load"]["power"], category_totals["Total_Load"]["cost"], "Electricity", "Total_Load", category_totals["Total_Load"]["net_energy"]))

    #         if current_timestamp.minute%15 == 0: 

    #             insert_query = """
    #                 INSERT INTO DGR_Data_15 (timedate, node, power, cost, type, category, net_energy)
    #                 VALUES (?, ?, ?, ?, ?, ?, ?)
    #             """
    #             # Execute the bulk insert
    #             cursor.execute(insert_query, (current_timestamp, "Total_Load", category_totals["Total_Load"]["power"], category_totals["Total_Load"]["cost"], "Electricity", "Total_Load", category_totals["Total_Load"]["net_energy"]))

    #     except pyodbc.Error as e:
    #         log_message(f"Error Processing Total Load Data: {traceback.format_exc()}")
    #         conn = connect_to_database(DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)  # Attempt to reconnect
    #         cursor = conn.cursor() if conn else None
    #     except Exception as e:
    #         log_message(f"Unexpected error inserting Total Load data: {traceback.format_exc()}")



    def allLoadData(cursor, current_timestamp, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD):
        try:
            cursor.execute("""
                SELECT node_name
                FROM Source_Info
                WHERE Source_type = 'Load'
                AND resource_type= 'Electricity'
            """)
            node_names = [row[0] for row in cursor.fetchall()]

            if not node_names:
                return  # No Load nodes found, nothing to do

            placeholders = ','.join('?' for _ in node_names)
            query = f"""
                SELECT node, power, cost, yesterday_net_energy, today_energy, net_energy
                FROM Source_Data
                WHERE node IN ({placeholders}) AND timedate = (SELECT MAX(timedate) FROM Source_Data)
            """
            cursor.execute(query, node_names)
            data_rows = cursor.fetchall()

            # Initialize total aggregation
            total = {"power": 0, "cost": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0}
            for node, power, cost, y_net, t_energy, net in data_rows:
                total["power"] += power
                total["cost"] += cost
                total["yesterday_net_energy"] += y_net
                total["today_energy"] += t_energy
                total["net_energy"] += net

            # Prepare common insert values
            source_data_tuple = (current_timestamp, "Total_Load", total["power"], total["cost"], total["yesterday_net_energy"], total["today_energy"], total["net_energy"])
            dgr_data_tuple = (current_timestamp, "Total_Load", total["power"], total["cost"], "Electricity", "Total_Load", total["net_energy"])

            # Execute inserts
            insert_source = """
                INSERT INTO Source_Data (timedate, node, power, cost, yesterday_net_energy, today_energy, net_energy)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(insert_source, source_data_tuple)

            insert_dgr = """
                INSERT INTO DGR_Data (timedate, node, power, cost, type, category, net_energy)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(insert_dgr, dgr_data_tuple)

            if current_timestamp.minute % 15 == 0:
                insert_dgr_15 = """
                    INSERT INTO DGR_Data_15 (timedate, node, power, cost, type, category, net_energy)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(insert_dgr_15, dgr_data_tuple)

        except pyodbc.Error:
            log_message(f"Error Processing Total Load Data: {traceback.format_exc()}")
            conn = connect_to_database(DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
            cursor = conn.cursor() if conn else None
        except Exception:
            log_message(f"Unexpected error inserting Total Load data: {traceback.format_exc()}")


    def checkAnyUpdate(cursor):
        """Update data if Source_Update indicates a change."""
        try:
            cursor.execute("SELECT updated FROM Source_Update")
            result = cursor.fetchone()


            if result and result[0]:  # If 'updated' is True
                cursor.execute("UPDATE Source_Update SET updated =0 ")  # Reset the update flag")
                log_message("Update triggered...")  # Placeholder for actual update logic
                return True
            else:
                return False
        except Exception as e:
            log_message(f"Error in update: {e}")


    def fetchLastData(cursor, node, not_connected_data_electricity):
        try:
            cursor.execute(
                'SELECT net_energy FROM Source_Data WHERE node=? AND timedate = (SELECT MAX(timedate) FROM Source_Data)', 
                (node,)
            )
            row = cursor.fetchone()
            net_energy = row[0] if row else 0.0
            if row:
                not_connected_data_electricity[node] = net_energy
            return net_energy

        except Exception as e:
            log_message(f'Error found in fetchLastData for electricity: {traceback.format_exc()}')
            return 0.0
        

    def superBusbarDataForElectricity(cursor, current_timestamp, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD):
        try:
            # Fetch Super_Bus_Bars and their connections
            cursor.execute("""
                SELECT id, node_name, connected_with
                FROM Source_Info
                WHERE Source_type = 'Super_Bus_Bar'
                AND resource_type = 'Electricity'
            """)
            super_busbars = cursor.fetchall()

            if not super_busbars:
                return

            # Fetch all ID -> (node_name, source_type) mappings
            cursor.execute("SELECT id, node_name, source_type FROM Source_Info")
            id_to_node = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

            # Fetch latest Source_Data for all nodes
            cursor.execute("""
                WITH Ranked AS (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY node ORDER BY timedate DESC) AS rn
                    FROM Source_Data
                )
                SELECT node, power, power_mod, cost, net_energy, today_energy, yesterday_net_energy, reactive_energy
                FROM Ranked WHERE rn = 1
            """)
            latest_data = {
                row[0]: {
                    'power': row[1] or 0.0,
                    'power_mod': row[2] or 0.0,
                    'cost': row[3] or 0.0,
                    'net_energy': row[4] or 0.0,
                    'today_energy': row[5] or 0.0,
                    'yesterday_net_energy': row[6] or 0.0,
                    'reactive_energy': row[7] or 0.0
                }
                for row in cursor.fetchall()
            }

            # Process each Super Bus Bar
            for _, node_name, connected_json in super_busbars:
                connected_ids = json.loads(connected_json or '[]')
                power = power_mod = cost = net_energy = today_energy = yesterday_net_energy = reactive_energy = 0.0

                for connected_id in connected_ids:
                    node_info = id_to_node.get(connected_id)
                    if not node_info:
                        continue

                    conn_node, conn_type = node_info
                    if conn_type in ('Bus_Bar', 'Meter_Bus_Bar', 'Load_Bus_Bar'):
                        data = latest_data.get(conn_node)
                        if data:
                            power += data['power']
                            power_mod += data['power_mod']
                            cost += data['cost']
                            net_energy += data['net_energy']
                            today_energy += data['today_energy']
                            yesterday_net_energy += data['yesterday_net_energy']
                            reactive_energy += data['reactive_energy']

                try:
                    monthly_pf = monthlyPfcalculation(cursor, current_timestamp, node_name, net_energy, reactive_energy, power)

                    cursor.execute("""
                        INSERT INTO Source_Data (timedate, node, power, voltage1, voltage2, voltage3, current1, current2, current3,
                                                frequency, power_factor, cost, power_mod, cost_mod, yesterday_net_energy, today_energy,
                                                net_energy, reactive_energy)
                        VALUES (?, ?, ?, 0, 0, 0, 0, 0, 0, 0, ?, ?, ?, 0, ?, ?, ?, ?)
                    """, (current_timestamp, node_name, power, monthly_pf, cost, power_mod, yesterday_net_energy, today_energy, net_energy, reactive_energy))

                    cursor.execute("""
                        INSERT INTO DGR_Data (timedate, node, power, cost, type, category)
                        VALUES (?, ?, ?, ?, 'Super_Bus_Bar', 'Electricity')
                    """, (current_timestamp, node_name, power, cost))

                    if current_timestamp.minute % 15 == 0:
                        cursor.execute("""
                            INSERT INTO DGR_Data_15 (timedate, node, power, cost, type, category)
                            VALUES (?, ?, ?, ?, 'Super_Bus_Bar', 'Electricity')
                        """, (current_timestamp, node_name, power, cost))

                except pyodbc.Error:
                    log_message(f"DB error inserting Super_Bus_Bar data for {node_name}: {traceback.format_exc()}")
                    conn = connect_to_database(DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
                    cursor = conn.cursor() if conn else None
                except Exception:
                    log_message(f"Unexpected error inserting Super_Bus_Bar data: {traceback.format_exc()}")

        except Exception:
            log_message(f"Critical error in superBusbarDataForElectricity: {traceback.format_exc()}")
        
    source_data_list = [
        {"timedate": "2025-05-26 14:37:37.290", "node": "REB", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "DG 01", "power": 11, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "DG 02", "power": 22, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Denim  Source", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Inspection  Unit", "power": 10.7264833984375, "voltage1": 222.504867553711, "voltage2": 221.822906494141, "voltage3": 225.092590332031, "current1": 10.9403886795044, "current2": 31.2826328277588, "current3": 7.27595710754395, "frequency": 50.1987762451172, "power_factor": 0, "cost": 0, "status": 1, "power_mod": 10.7264833984375, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 13750.3032421833, "net_energy": 13750.3032421833, "color": "#6CE759", "reactive_energy": 1212.89622292763},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Sanforizing  01", "power": 3.48651098632813, "voltage1": 232.57942199707, "voltage2": 231.265762329102, "voltage3": 231.873504638672, "current1": 6.09163856506348, "current2": 5.58986759185791, "current3": 5.65670776367188, "frequency": 50.0346908569336, "power_factor": 0, "cost": 0, "status": 1, "power_mod": 3.48651098632813, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 18561.311239275, "net_energy": 18561.311239275, "color": "#6CE759", "reactive_energy": 1554.50128962443},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Floor  Light-30", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Roof Top  Fan-10", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Padding-01", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Padding-02", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "X-Raite", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Padding-03", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Universal  Strength  Tester", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Rubbing  Tester", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Incubator", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Dark Room  E-Box", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Wash-01", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Wash-02", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Dryer", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "CPB", "power": 2.57743017578125, "voltage1": 230.770721435547, "voltage2": 231.602615356445, "voltage3": 231.309982299805, "current1": 5.1713809967041, "current2": 5.25435400009155, "current3": 5.81229257583618, "frequency": 50.0313568115234, "power_factor": 0, "cost": 0, "status": 1, "power_mod": 2.57743017578125, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 8509.08015561448, "net_energy": 8509.08015561448, "color": "#6CE759", "reactive_energy": 8366.64245655611},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Mixer Tank", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Pad Steam", "power": 18.94854296875, "voltage1": 231.472732543945, "voltage2": 232.443481445313, "voltage3": 231.533721923828, "current1": 32.7918090820313, "current2": 29.9019813537598, "current3": 39.4502716064453, "frequency": 50.0388031005859, "power_factor": 0, "cost": 2413.55826659554, "status": 1, "power_mod": 18.94854296875, "cost_mod": 2413.55826659554, "yesterday_net_energy": 47432.8256395762, "today_energy": 249.077220494895, "net_energy": 47681.9028600711, "color": "#6CE759", "reactive_energy": 23073.8649692401},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Washing", "power": 6.2846611328125, "voltage1": 223.577392578125, "voltage2": 223.395034790039, "voltage3": 225.756942749023, "current1": 11.576681137085, "current2": 11.1464881896973, "current3": 9.12502098083496, "frequency": 50.2003326416016, "power_factor": 0, "cost": -1020254.01971256, "status": 1, "power_mod": 6.2846611328125, "cost_mod": 1020254.01971256, "yesterday_net_energy": 158764.924655944, "today_energy": -105289.372519356, "net_energy": 53475.5521365888, "color": "#6CE759", "reactive_energy": 18558.4120420417},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Mercerizing", "power": 69.6482109375, "voltage1": 224.844543457031, "voltage2": 223.392044067383, "voltage3": 223.065032958984, "current1": 127.609275817871, "current2": 122.367080688477, "current3": 113.417213439941, "frequency": 50.2000694274902, "power_factor": 0, "cost": 1558.99424770649, "status": 1, "power_mod": 69.6482109375, "cost_mod": 1558.99424770649, "yesterday_net_energy": 120173.754356393, "today_energy": 160.886919267956, "net_energy": 120334.641275661, "color": "#6CE759", "reactive_energy": 58922.9111294097},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Submersible   Pump-02  (EM)", "power": 0.0234321613311768, "voltage1": 225.02424621582, "voltage2": 222.911209106445, "voltage3": 223.226791381836, "current1": 0.177725359797478, "current2": 0.168512761592865, "current3": 0.0355318523943424, "frequency": 50.200553894043, "power_factor": 0, "cost": 689.716289613834, "status": 1, "power_mod": 0.0234321613311768, "cost_mod": 689.716289613834, "yesterday_net_energy": 28396.0606522026, "today_energy": 71.1781516629344, "net_energy": 28467.2388038655, "color": "#6CE759", "reactive_energy": 1967.89813622611},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Bruckner  01", "power": 7.681724609375, "voltage1": 222.323486328125, "voltage2": 224.613800048828, "voltage3": 222.579849243164, "current1": 16.6750583648682, "current2": 9.85283946990967, "current3": 14.3229122161865, "frequency": 50.2000389099121, "power_factor": 0, "cost": 106.292960453958, "status": 1, "power_mod": 7.681724609375, "cost_mod": 106.292960453958, "yesterday_net_energy": 174460.228922308, "today_energy": 10.9693457640824, "net_energy": 174471.198268072, "color": "#6CE759", "reactive_energy": 35865.4081985296},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Bruckner-02", "power": 82.913671875, "voltage1": 221.885879516602, "voltage2": 224.505187988281, "voltage3": 222.242919921875, "current1": 139.116912841797, "current2": 142.507064819336, "current3": 127.298065185547, "frequency": 50.1971092224121, "power_factor": 0, "cost": 9361.12072160287, "status": 1, "power_mod": 82.913671875, "cost_mod": 9361.12072160287, "yesterday_net_energy": 165369.919763657, "today_energy": 966.059929989977, "net_energy": 166335.979693647, "color": "#6CE759", "reactive_energy": 29133.0617588362},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Implan  Thermo Oil  Boiler", "power": 45.8385, "voltage1": 223.0712890625, "voltage2": 218.038955688477, "voltage3": 221.169342041016, "current1": 76.8595809936523, "current2": 77.1570587158203, "current3": 73.4895477294922, "frequency": 50.1996650695801, "power_factor": 0, "cost": 6331.2027931423, "status": 1, "power_mod": 45.8385, "cost_mod": 6331.2027931423, "yesterday_net_energy": 100525.463465575, "today_energy": 653.374901253075, "net_energy": 101178.838366828, "color": "#6CE759", "reactive_energy": 51315.4754074413},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Thermosol", "power": 8.215875, "voltage1": 231.955780029297, "voltage2": 232.806442260742, "voltage3": 232.632202148438, "current1": 13.830135345459, "current2": 13.5390491485596, "current3": 13.9668731689453, "frequency": 50.0409164428711, "power_factor": 0, "cost": 2616.3672641106, "status": 1, "power_mod": 8.215875, "cost_mod": 2616.3672641106, "yesterday_net_energy": 61771.5613826971, "today_energy": 270.006941600681, "net_energy": 62041.5683242978, "color": "#6CE759", "reactive_energy": 28133.4933329694},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Sanforizing", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "WTP", "power": 22.689796875, "voltage1": 224.094985961914, "voltage2": 222.173492431641, "voltage3": 222.804183959961, "current1": 44.7209587097168, "current2": 44.8203582763672, "current3": 42.6404151916504, "frequency": 50.1983299255371, "power_factor": 0, "cost": 3492.25151336518, "status": 1, "power_mod": 22.689796875, "cost_mod": 3492.25151336518, "yesterday_net_energy": 64408.3778073707, "today_energy": 360.397472999503, "net_energy": 64768.7752803702, "color": "#6CE759", "reactive_energy": 15629.2864492773},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Control  Panel", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Cockram  Gas Boiler", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Steam Coal  Boiler 02", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Thermo  Coal Boiler", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Steam Coal  Boiler 01", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Dante  Peach", "power": 0.462686462402344, "voltage1": 225.798156738281, "voltage2": 224.356231689453, "voltage3": 223.599136352539, "current1": 1.50995910167694, "current2": 1.92669010162354, "current3": 1.49429607391357, "frequency": 50.2073364257813, "power_factor": 0, "cost": 206.583414430643, "status": 1, "power_mod": 0.462686462402344, "cost_mod": 206.583414430643, "yesterday_net_energy": 35692.6035255969, "today_energy": 21.3192378153399, "net_energy": 35713.9227634123, "color": "#6CE759", "reactive_energy": 10858.1289230658},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Warping", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Gas  singeing", "power": 3.43976806640625, "voltage1": 225.214965820313, "voltage2": 223.763687133789, "voltage3": 223.372665405273, "current1": 4.95184946060181, "current2": 5.78254985809326, "current3": 5.49971199035645, "frequency": 50.2001113891602, "power_factor": 0, "cost": 905.868456532263, "status": 1, "power_mod": 3.43976806640625, "cost_mod": 905.868456532263, "yesterday_net_energy": 27549.7813192884, "today_energy": 93.4848768351148, "net_energy": 27643.2661961236, "color": "#6CE759", "reactive_energy": 15263.2415618776},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Jigger", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Electro  Coagulation  02", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Roof Top  Fan-13", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Daf Skimmer", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Lafer  Peach-01", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Electro   Coagulation  01", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "EFFluent  Treatment", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Lafer  Peach-02", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Sand Filter", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Un-Roling", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Bleaching", "power": 47.86305078125, "voltage1": 224.257202148438, "voltage2": 223.048370361328, "voltage3": 222.595718383789, "current1": 92.3582763671875, "current2": 81.7149810791016, "current3": 80.6942367553711, "frequency": 50.19921875, "power_factor": 0, "cost": 3191.00332268417, "status": 1, "power_mod": 47.86305078125, "cost_mod": 3191.00332268417, "yesterday_net_energy": 74689.1485582912, "today_energy": 329.308908429739, "net_energy": 75018.457466721, "color": "#6CE759", "reactive_energy": 32731.986442246},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Finger 02", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Submersible  Pump-01  (EM)", "power": 0.144998046875, "voltage1": 224.560729980469, "voltage2": 223.13639831543, "voltage3": 222.347045898438, "current1": 0.429368078708649, "current2": 0.517646908760071, "current3": 0.195158988237381, "frequency": 50.2043342590332, "power_factor": 0, "cost": 3049.68336673584, "status": 1, "power_mod": 0.144998046875, "cost_mod": 3049.68336673584, "yesterday_net_energy": 39319.4365639781, "today_energy": 314.724805648693, "net_energy": 39634.1613696267, "color": "#6CE759", "reactive_energy": 717.105203597734},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Punch 02", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Punch 04", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Finger 01", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Punch 01", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Punch 03", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Rolling-01", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Rolling-02", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Rolling-03", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Rolling-04", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Rolling-05", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Rolling-06", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Rolling-07", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Rolling-08", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Rolling-09", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Rolling-10", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy": 0},
        {"timedate": "2025-05-26 14:37:37.290", "node": "Yarn  DyeingDB", "power": 0, "voltage1": 0, "voltage2": 0, "voltage3": 0, "current1": 0, "current2": 0, "current3": 0, "frequency": 0, "power_factor": 0, "cost": 0, "status": 0, "power_mod": 0, "cost_mod": 0, "yesterday_net_energy": 0, "today_energy": 0, "net_energy": 0, "color": "#6CE759", "reactive_energy":0}
,{
        "timedate": "2025-05-26 14:37:37.290",
        "node": "MDB",
        "power": 564.941125,
        "voltage1": 224.591659545898,
        "voltage2": 225.189239501953,
        "voltage3": 226.156402587891,
        "current1": 847.808166503906,
        "current2": 901.597473144531,
        "current3": 922.828796386719,
        "frequency": 50.2054443359375,
        "power_factor": 0,
        "cost": 56681.9398952738,
        "status": 1,
        "power_mod": 564.941125,
        "cost_mod": 56681.9398952738,
        "yesterday_net_energy": 1495775.86039792,
        "today_energy": 5849.52940095705,
        "net_energy": 1501625.38979888,
        "color": "#6CE759",
        "reactive_energy": 584523.419376806
    },
    {
        "timedate": "2025-05-26 14:37:37.290",
        "node": "SDB-05 Laboratory",
        "power": 6.526380859375,
        "voltage1": 226.553833007813,
        "voltage2": 223.27815246582,
        "voltage3": 224.874526977539,
        "current1": 9.599684715271,
        "current2": 13.5633115768433,
        "current3": 8.38004875183105,
        "frequency": 50.2023315429688,
        "power_factor": 0,
        "cost": 1347.66381238407,
        "status": 1,
        "power_mod": 6.526380859375,
        "cost_mod": 1347.66381238407,
        "yesterday_net_energy": 25499.9742700407,
        "today_energy": 139.077792815693,
        "net_energy": 25639.0520628564,
        "color": "#6CE759",
        "reactive_energy": 122.190693247644
    },
    {
        "timedate": "2025-05-26 14:37:37.290",
        "node": "DB-1 Gas Boiler",
        "power": 80.8144140625,
        "voltage1": 233.841461181641,
        "voltage2": 233.272796630859,
        "voltage3": 231.603332519531,
        "current1": 134.705017089844,
        "current2": 102.324325561523,
        "current3": 125.957038879395,
        "frequency": 50.0200157165527,
        "power_factor": 0,
        "cost": 0,
        "status": 1,
        "power_mod": 80.8144140625,
        "cost_mod": 0,
        "yesterday_net_energy": 0,
        "today_energy": 159567.189177192,
        "net_energy": 159567.189177192,
        "color": "#6CE759",
        "reactive_energy": 53115.7774148355
    }
        ]
except Exception as e:
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"Error : {traceback.format_exc()}\n")
