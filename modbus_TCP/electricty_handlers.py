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
    #             SELECT id, node_name, source_type, connected_with
    #             FROM Source_Info
    #             WHERE Source_type IN ('Bus_Bar', 'Load_Bus_Bar')
    #             AND resource_type = 'Electricity'
    #         """)
    #         busbars = [row for row in cursor.fetchall() if row[1] not in ['Others 1', 'Others 2', 'Others 3', 'Others 4']]

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
    #             try:
    #                 pf = monthlyPfcalculation(cursor, current_timestamp, node_name, aggregates['net_energy'], aggregates['reactive_energy'], aggregates['power'])
    #                 cursor.execute("""
    #                     INSERT INTO Source_Data (timedate, node, power, voltage1, voltage2, voltage3, current1, current2, current3,
    #                                             frequency, power_factor, cost, power_mod, cost_mod, yesterday_net_energy, today_energy,
    #                                             net_energy, reactive_energy)
    #                     VALUES (?, ?, ?, 0, 0, 0, 0, 0, 0, 0, ?, ?, ?, 0, ?, ?, ?, ?)
    #                 """, (current_timestamp, node_name, aggregates['power'], pf, aggregates['cost'], aggregates['power_mod'],
    #                     aggregates['yesterday_net_energy'], aggregates['today_energy'], aggregates['net_energy'], aggregates['reactive_energy']))

    #                 cursor.execute("""
    #                     INSERT INTO DGR_Data (timedate, node, power, cost, type, category)
    #                     VALUES (?, ?, ?, ?, ?, 'Electricity')
    #                 """, (current_timestamp, node_name, aggregates['power'], aggregates['cost'], source_type))

    #                 if current_timestamp.minute % 15 == 0:
    #                     cursor.execute("""
    #                         INSERT INTO DGR_Data_15 (timedate, node, power, cost, type, category)
    #                         VALUES (?, ?, ?, ?, ?, 'Electricity')
    #                     """, (current_timestamp, node_name, aggregates['power'], aggregates['cost'], source_type))

    #             except pyodbc.Error:
    #                 log_message(f"DB error inserting data for {node_name}: {traceback.format_exc()}")
    #                 conn = connect_to_database(DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
    #                 cursor = conn.cursor() if conn else None
    #             except Exception:
    #                 log_message(f"Unexpected error inserting data: {traceback.format_exc()}")

    #     except Exception:
    #         log_message(f"Critical error in busbarDataForElectricity: {traceback.format_exc()}")


    busbars = {}
    def busbarDataForElectricity(cursor, current_timestamp, source_data_list):
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
            dataset = {item[1]: item[2] for item in source_data_list}
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
            source_data_list.clear()

            # pf = monthlyPfcalculation(cursor, current_timestamp, node_name, aggregates['net_energy'], aggregates['reactive_energy'], aggregates['power'])
            if busbar_data_list:
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
            log_message(f"Critical error in busbarDataForElectricity: {traceback.format_exc()}")
            raise
        except Exception:
            log_message(f"Critical error in busbarDataForElectricity: {traceback.format_exc()}")



    def recursionFunction(node, source_type, id_to_node, dataset, connected_with, load_list):
        value=0
        if dataset.get(node, None) is not None:
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


    def allSourceData(cursor, current_timestamp):
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
            if bulk_data_source_data:
                insert_source_query = """
                    INSERT INTO Source_Data (timedate, node, power, cost, yesterday_net_energy, today_energy, net_energy)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                cursor.executemany(insert_source_query, bulk_data_source_data)

            # Insert into DGR_Data
            if bulk_data_dgr:
                insert_dgr_query = """
                    INSERT INTO DGR_Data (timedate, node, power, cost, type, category, net_energy)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                cursor.executemany(insert_dgr_query, bulk_data_dgr)

            if current_timestamp.minute % 15 == 0 and bulk_data_dgr:
                insert_dgr_15_query = """
                    INSERT INTO DGR_Data_15 (timedate, node, power, cost, type, category, net_energy)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                cursor.executemany(insert_dgr_15_query, bulk_data_dgr)

        except pyodbc.Error:
            log_message(f"Error Processing Total Source Data: {traceback.format_exc()}")
            raise
        except Exception:
            log_message(f"Error Processing Total Source Data: {traceback.format_exc()}")


    def getYesterdayEnergyAndCost(cursor, energy_store):
        try:
            # Fetch all node names from Source_Info
            cursor.execute("""
                SELECT node_name 
                FROM Source_Info
                WHERE resource_type = 'Electricity'
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
        except pyodbc.Error as e:
            log_message(f"Database error fetching yesterday's energy and cost: {traceback.format_exc()} {e}")
            raise
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

            return cost

        except Exception as e:
            log_message(f'Error found in the costCalculation function {traceback.format_exc()}')
            raise


    def readNode(cursor,current_timestamp, temp, node_name, category, source_type, machine_max_power, energy_store, max_limit_count, source_data_list, dgr_data_list, dgr_data_15_list):

        try:
                global not_conn_elec
  
                #will be optimize later
                # for notification
                if temp['Net_Energy']==0:
                    temp['Net_Energy']= not_conn_elec.get(node_name, None)
                    if temp['Net_Energy'] is None:
                        temp['Net_Energy'] = fetchLastData(cursor, node_name, not_conn_elec)
                else:
                    not_conn_elec.pop(node_name, None)

                # monthly_pf= monthlyPfcalculation(cursor, current_timestamp, node_name, temp["Net_Energy"], temp['Reactive_Energy'],temp['Power'])
                monthly_pf=0
                
        
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
            cursor.execute("SELECT zlan_ip, node_name, category, source_type, machine_max_power,meter_no FROM Source_Info WHERE resource_type= 'Electricity' AND source_type IN ('Source', 'Load', 'Meter_Bus_Bar', 'LB_Meter') AND connection_type = 'Zlan'")
            rows = cursor.fetchall()
            results = []

            category_dict = {}
            source_type_dict = {}
            machine_max_power_dict = {}


            meter_dict = {}  # Initialize the dictionary to store meter_no as key and node_name as value

            for row in rows:
                zlan_ip, node_name, category, source_type, machine_max_power, meter_no = row
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
            cursor.execute("SELECT ip3, ip4_unit_id, meter_model, node_name, category, source_type, machine_max_power FROM Source_Info WHERE resource_type= 'Electricity' AND source_type IN ('Source', 'Load', 'Meter_Bus_Bar', 'LB_Meter') AND connection_type = 'TCP/IP'")
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


    def processReadNodeForElectricity(cursor, current_timestamp, results, category_dict, source_type_dict, machine_max_power_dict, energy_store, max_limit_count, source_data_list, dgr_data_list, dgr_data_15_list, generators):
        checkGeneratorStatus(results, generators)
        for data in results:
            node_name = data['Node_Name']

            category = category_dict.get(node_name)
            source_type = source_type_dict.get(node_name)
            machine_max_power = machine_max_power_dict.get(node_name)

            readNode(cursor, current_timestamp, data, data['Node_Name'], category, source_type, machine_max_power, energy_store, max_limit_count, source_data_list, dgr_data_list, dgr_data_15_list)

    def bulkInsertForElectricity(cursor, source_data_list, dgr_data_list, dgr_data_15_list):
        try:
            
            if source_data_list:
                cursor.executemany('''
                    INSERT INTO Source_Data (timedate, node, power, voltage1, voltage2, voltage3, current1, current2, current3, frequency, power_factor, cost, status, power_mod, cost_mod, yesterday_net_energy, today_energy, net_energy, color, reactive_energy)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', source_data_list)
                # source_data_list.clear()  # Clear the list after insertion

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
            log_message(f"Unexpected error during bulk insert for electricity: {traceback.format_exc()} {e}")
            source_data_list.clear()
            dgr_data_list.clear()     
            dgr_data_15_list.clear()
            raise
        except Exception:
            log_message(f"Unexpected error during bulk insert for electricity: {traceback.format_exc()}")
            source_data_list.clear()
            dgr_data_list.clear()     
            dgr_data_15_list.clear()

 


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
                    COUNT(DISTINCT CASE WHEN power != 0 THEN FORMAT(timedate, 'yyyy-MM-dd HH:mm') END) AS runtime
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
        except pyodbc.Error as e:
            log_message(f"Database error in electricityMonthlyInsertion: {traceback.format_exc()}")
            raise
        except Exception as e:
            log_message("Error in electricityMonthlyInsertion: " + traceback.format_exc())




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

            for node in energy_data.keys():
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
        except pyodbc.Error as e:
            log_message(f"Database error in electricity yearly insertion: {traceback.format_exc()}")
            raise
        except Exception as e:
            log_message(f"Error in electricity yearly insertion: {traceback.format_exc()}")




    def allLoadData(cursor, current_timestamp):
        try:
            cursor.execute("""
                SELECT node_name
                FROM Source_Info
                WHERE Source_type = 'Load'
                AND resource_type= 'Electricity'
            """)
            node_names = [row[0] for row in cursor.fetchall()]
            if not node_names:
                return

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
            raise
        except Exception as e:
            log_message(f"Error Processing Total Load Data: {traceback.format_exc()} {e}")


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
            log_message(f"Error in checkAnyUpdate: {e}")
            raise


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

    def minuteChecker(last_run_minute):
        reset=True
        while reset:
            current_time=datetime.datetime.now()
            current_minute=current_time.replace(second=0, microsecond=0)
            # log_message(f"last_minute: {last_run_minute} current_minute: {current_minute} current_time: {current_time}")
            if last_run_minute!=current_minute:
                last_run_minute=current_minute
                reset=False
                # log_message(f"new minute detected, last_run_minute updated to {last_run_minute}")
            else:
                sleep(20)
        return last_run_minute
    
    # def slaveInfoElectricity(cursor):
    #     try:
    #         cursor.execute("SELECT zlan_ip, meter_no, meter_model FROM Source_Info WHERE resource_type= 'Electricity' AND connection_type = 'Zlan'")
    #         rows = cursor.fetchall()
    #         slave_info_zlan={}
    #         for row in rows:
    #             zlan_ip, meter_no, meter_model = row
    #             if zlan_ip not in slave_info_zlan:
    #                 slave_info_zlan[zlan_ip]={}
    #             slave_info_zlan[zlan_ip][meter_no]= meter_model
    #         return slave_info_zlan

    #     except Exception as e:
    #         log_message(f"Error in slaveInfoElectricity: {traceback.format_exc()}")
    #         return []
    def slaveIpAndModelMap(cursor):
        try:
            cursor.execute("SELECT zlan_ip, meter_no, meter_model FROM Source_Info WHERE connection_type = 'Zlan'")
            rows = cursor.fetchall()
            slave_info_zlan={}
            for row in rows:
                zlan_ip, meter_no, meter_model = row
                if zlan_ip not in slave_info_zlan:
                    slave_info_zlan[zlan_ip]={}
                slave_info_zlan[zlan_ip][meter_no]= meter_model
            return slave_info_zlan

        except pyodbc.Error as e:
            log_message(f"DB error in slaveIpAndModelMap: {traceback.format_exc()} {e}")
            raise
        except Exception as e:
            log_message(f"Error in slaveIpAndModelMap: {traceback.format_exc()} {e}")
            return []
        
    def fetchGenerators(cursor):
        try:
            cursor.execute("""
                SELECT node_name 
                FROM Source_Info 
                WHERE category IN ('Gas_Generator', 'Diesel_Generator')
            """)
            rows = cursor.fetchall()

            # Return as a set for fast lookups
            generators = {row[0] for row in rows}
            return generators
        except pyodbc.Error as e:
            log_message(f"DB error in fetchGenerators: {traceback.format_exc()} {e}")
            raise
        except Exception:
            log_message(f"Error in fetchGenerators: {traceback.format_exc()}")
            return set()
    def checkGeneratorStatus(results, generators):
        try:
            global generator_is_running
            generator_is_running = any(
                data['Power'] > 0
                for data in results
                if data['Node_Name'] in generators
            )
        except Exception:
            log_message(f"Error in checkGeneratorStatus: {traceback.format_exc()}")
        

except Exception as e:
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"Error : {traceback.format_exc()}\n")
