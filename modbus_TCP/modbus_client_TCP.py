
import asyncio
from pymodbus.client import AsyncModbusTcpClient
from datetime import datetime
import struct
import pyodbc
from dotenv import load_dotenv
import os
import websocket
import json
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian

load_dotenv()


log_file_path = os.getenv('LOG_FILE_PATH')

def log_message(message):
        with open(log_file_path, 'a') as log_file:
            log_file.write(f"{datetime.now()}: {message}\n")


# token='4d07820f27d23a02eca124d5287e7e8eb32cc550' 
# websocket_url = "ws://192.168.68.61:8086/ws/test/"  # Adjust as necessary
# websocket_url = "ws://203.95.221.58:8086/ws/live-notification/"
# ws = websocket.WebSocket()


# def send_notification(node_name,power=0):
#     try:
#         notification_data = {
#             "node_name": node_name,
#             "power":power,
#         }
#         ws.send(json.dumps(notification_data))
#         print(f"Notification sent for node: {node_name}")
#     except Exception as e:
#         print(f"Error sending notification: {e}")




connection_string = (
    f"DRIVER={'ODBC Driver 17 for SQL Server'};"
    f"SERVER={os.getenv('DB_SERVER')};"
    f"DATABASE={os.getenv('DB_NAME')};"
    f"UID={os.getenv('DB_UID')};"
    f"PWD={os.getenv('DB_PWD')};"
)




# connection_string = (
#         "DRIVER={ODBC Driver 17 for SQL Server};"
#         "SERVER=DESKTOP-E2AADH4;"  # Replace with your DB server IP
#         "DATABASE=TCP_Data;"  # Replace with your DB name
#         "UID=sa;"  # Replace with your username
#         "PWD=123456;"  # Replace with your password
#     )

# Retry function wrapper
async def retry_on_failure(func, retries=5, delay=2, *args, **kwargs):
    for attempt in range(1, retries + 1):
        try:
            return await func(*args, **kwargs)
        except asyncio.TimeoutError:
            print(f"Timeout error occurred. Retry {attempt} of {retries}...")
            if attempt < retries:
                await asyncio.sleep(delay)
            else:
                print("Maximum retries reached. Giving up.")
                raise
        except Exception as e:
            print(f"Unexpected error: {e}. Retry {attempt} of {retries}...")
            if attempt < retries:
                await asyncio.sleep(delay)
            else:
                print("Maximum retries reached. Giving up.")
                raise



def insert_data_to_mssql(data,slave_ip):
    """
    Inserts M1M20 data into the MSSQL database.

    :param data: Dictionary containing the M1M20 data.
    :param connection_string: Connection string for MSSQL.
    """
    try:
        global connection_string
        # Establish database connection
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO dbo.Source_Data_New (
            node_name, 
            timestamp, 
            active_power, 
            pv_l1, 
            pv_l2, 
            pv_l3, 
            current_l1, 
            current_l2, 
            current_l3, 
            frequency,
            net_energy,
            status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        # Insert data into the table
        cursor.execute(insert_query, (
                            data.get("node_name"),
                            data.get("timestamp"),
                            data.get("active_power"),
                            data.get("pv_l1"),
                            data.get("pv_l2"),
                            data.get("pv_l3"),
                            data.get("current_l1"),
                            data.get("current_l2"),
                            data.get("current_l3"),
                            data.get("frequency"),
                            data.get("net_energy"),
                            data.get("status")
                        ))

        # Commit the transaction
        conn.commit()

    except Exception as e:
        print(f"Database operation failed: {e}")
    finally:
        if conn:
            conn.close()


def convertRegistersToDataM1M20_4(register1, register2, register3, register4, resolution):
    # Combine four registers into one hex value
    combined_value = int(f"{register1:04x}{register2:04x}{register3:04x}{register4:04x}", 16)
    # Multiply by resolution and round to 3 decimal places
    return round(combined_value * resolution, 3)

def convertRegistersToDataM1M20_2(register1, register2, resolution):
    combined_value = int((hex(register1) + hex(register2)[2:]), 16)
    return round(combined_value * resolution,3)


# Function to read Modbus data
async def readModbusDataM1M20(client, prev_timestamp=None,slave_ip=None,node_name=None):
    current_time = datetime.now()
    formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    # formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

    data = {}



    try:
        # Call Modbus function with retries
        response_data = await retry_on_failure(client.read_holding_registers, retries=3, delay=2, address=23298, count=100)

        response_energy = await retry_on_failure(client.read_holding_registers, retries=3, delay=2, address=20480, count=16)
        
        if not response_data.isError():
            registers_data = response_data.registers
            registers_energy = response_energy.registers

            # print(registers)

            data["Node_Name"] = node_name
            data["Power"] = convertRegistersToDataM1M20_2(registers_data[24], registers_data[25], 0.00001)
            data["Voltage1"] = convertRegistersToDataM1M20_2(registers_data[0], registers_data[1], 0.1)
            data["Voltage2"] = convertRegistersToDataM1M20_2(registers_data[2], registers_data[3], 0.1)
            data["Voltage3"] = convertRegistersToDataM1M20_2(registers_data[4], registers_data[5], 0.1)
            data["Current1"] = convertRegistersToDataM1M20_2(registers_data[14], registers_data[15], 0.01)
            data["Current2"] = convertRegistersToDataM1M20_2(registers_data[16], registers_data[17], 0.01)
            data["Current3"] = convertRegistersToDataM1M20_2(registers_data[18], registers_data[19], 0.01)
            data["Frequency"] = convertRegistersToDataM1M20_2(registers_data[54], registers_data[55], 0.01)
            active_energy_positive = convertRegistersToDataM1M20_4(registers_energy[0], registers_energy[1], registers_energy[2], registers_energy[3], 0.01)
            active_energy_negative = convertRegistersToDataM1M20_4(registers_energy[4], registers_energy[5], registers_energy[6], registers_energy[7], 0.01)
            data["Net_Energy"] = active_energy_positive - active_energy_negative
            reactive_energy_positive = convertRegistersToDataM1M20_4(registers_energy[8], registers_energy[9], registers_energy[10], registers_energy[11], 0.01)
            reactive_energy_negative = convertRegistersToDataM1M20_4(registers_energy[12], registers_energy[13], registers_energy[14], registers_energy[15], 0.01)
            data["Reactive_Energy"] = reactive_energy_positive - reactive_energy_negative
            data["Status"] = 0 if data["Power"] == 0 else 1



            # insert_data_to_mssql(data,slave_ip)


        else:
            print(f"Modbus error: {response_data}")
    except asyncio.TimeoutError:
        print(f"Failed to retrieve data from {client.host} after retries.")

    except Exception as e:
        print(f"Unexpected failure: {e}")


    return data



#NEED FOR CLIENT_ELECTRICTY
def convert_int16_to_float(registers, byteorder=Endian.Big, wordorder=Endian.Big):
    """
    Convert two 16-bit Modbus registers into a 32-bit floating point number.

    :param registers: List of two 16-bit integers (e.g., [high_word, low_word])
    :param byteorder: Byte order (default: Big Endian)
    :param wordorder: Word order (default: Big Endian)
    :return: Converted 32-bit float value
    """
    # if len(registers) != 2:
    #     raise ValueError("Expected exactly two 16-bit registers.")

    decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=byteorder, wordorder=wordorder)
    return decoder.decode_32bit_float()
    
#NEED FOR CLIENT_ELECTRICTY
def convert_int16_to_64float(registers, byteorder=Endian.Big, wordorder=Endian.Big):
    """
    Convert two 16-bit Modbus registers into a 32-bit floating point number.

    :param registers: List of two 16-bit integers (e.g., [high_word, low_word])
    :param byteorder: Byte order (default: Big Endian)
    :param wordorder: Word order (default: Big Endian)
    :return: Converted 32-bit float value
    """
    # if len(registers) != 2:
    #     raise ValueError("Expected exactly two 16-bit registers.")

    decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=byteorder, wordorder=wordorder)
    return decoder.decode_64bit_float()   
#NEED FOR CLIENT_ELECTRICTY


async def readModbusDataPAC3220(client, prev_timestamp=None,slave_ip=None,node_name=None):

    data = {}



    try:
        # Call Modbus function with retries
        response = await retry_on_failure(client.read_holding_registers, retries=3, delay=2, address=64, count=2, slave=156)
        response_energy = await retry_on_failure(client.read_holding_registers, retries=3, delay=2, address=801, count=20, slave=156)
        
        if not response.isError() or not response_energy.isError():
            registers = response.registers
            registers_energy = response_energy.registers

            # print(registers)
            data["Node_Name"] = node_name
            data["Power"] = convert_int16_to_float(registers[64:66])/1000
            data["Voltage1"] = convert_int16_to_float(registers[0:2])
            data["Voltage2"] = convert_int16_to_float(registers[2:4])
            data["Voltage3"] = convert_int16_to_float(registers[4:6])
            data["Current1"] = convert_int16_to_float(registers[12:14])
            data["Current2"] = convert_int16_to_float(registers[14:16])
            data["Current3"] = convert_int16_to_float(registers[16:18])
            data["Frequency"] = convert_int16_to_float(registers[54:56])
            data["Net_Energy"] = convert_int16_to_64float(registers_energy[0:4])/1000 - convert_int16_to_64float(registers_energy[8:12])/1000
            data["Reactive_Energy"] = convert_int16_to_64float(registers_energy[16:20])/1000
            data["Status"] = 0 if data["Power"] == 0 else 1





            return data



            # insert_data_to_mssql(data,slave_ip)
            # send_notification(node_name,data["Active_Power"])



        else:
            print(f"Modbus error: {response}")
    except asyncio.TimeoutError:
        print(f"Failed to retrieve data from {client.host} after retries.")

    except Exception as e:
        print(f"Unexpected failure: {e}")


# # Polling function
# #NEED FOR CLIENT_ELECTRICTY
# async def poll_modbus_slaves(slave_info, slave_port):
#     prev_timestamp = None  # Initialize previous timestamp
#     for item in slave_info:

#         print(f"Connecting to Modbus Slave at {item['slave_ip']}")
#         async with AsyncModbusTcpClient(item['slave_ip'], port=slave_port) as client:
#             if client:
#                 # print(client)
#                 if item['meter_type'] == "M1M20":
#                     data, prev_timestamp = await readModbusDataM1M20(client, prev_timestamp,item['slave_ip'],node_name=item['node_name'])
#                 elif item['meter_type'] == "PAC3220":       
#                     data, prev_timestamp = await readModbusDataPAC3220(client, prev_timestamp,item['slave_ip'],node_name=item['node_name'])

#             else:
#                 print(f"Failed to connect to Modbus Slave at {item['slave_ip']}")

not_connected_data={}
def takeDataFromDict(item, not_connected_data):
    try:
        data = {}

        # Use get() to avoid KeyError
        net_energy = not_connected_data.get(item['node_name'])

        if net_energy:  # If net_energy is found and truthy
            data["Node_Name"] = item['node_name']
            data["Power"] = 0.0
            data["Voltage1"] = 0.0
            data["Voltage2"] = 0.0
            data["Voltage3"] = 0.0
            data["Current1"] = 0.0
            data["Current2"] = 0.0
            data["Current3"] = 0.0
            data["Frequency"] = 0.0
            data["Net_Energy"] = net_energy  # net_energy will be assigned directly
            data["Reactive_Energy"] = 0.0
            data["Status"] = 0 if data["Power"] == 0 else 1

            return data
        else:
            return None  # Return None if no valid net_energy found

    except Exception as e:
        log_message(f"Error in function takeDataFromDict: {e}")

def fetchLastData(item, conn, not_connected_data):
    try:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT net_energy FROM Source_Data WHERE node=? AND timedate = (SELECT MAX(timedate) FROM Source_Data)', 
            (item["node_name"],)
        )
        row = cursor.fetchone()
        net_energy = row[0] if row else 0.0

        data = {
            "Node_Name": item["node_name"],
            "Power": 0.0,
            "Voltage1": 0.0, "Voltage2": 0.0, "Voltage3": 0.0,
            "Current1": 0.0, "Current2": 0.0, "Current3": 0.0,
            "Frequency": 0.0,
            "Net_Energy": net_energy,
            "Reactive_Energy": 0.0,
            "Status": 0
        }
        if row:
            not_connected_data[item["node_name"]] = net_energy
        cursor.close()
        return data

    except Exception as e:
        log_message(f'Error found in fetchLastData: {e}')


async def poll_modbus_slaves(slave_info, slave_port, conn):
    """Polls multiple Modbus slaves concurrently."""

    tasks = []  # Store async tasks

    for item in slave_info:

        task = asyncio.create_task(process_slave(item, slave_port, conn))
        tasks.append(task)

    # Wait for all tasks to complete
    results = await asyncio.gather(*tasks)

    # Filter out None results
    results = [result for result in results if result is not None]

    return results  # List of data dictionaries

async def process_slave(item, slave_port, conn):
    global not_connected_data
    """Handles the connection and reading of a single Modbus slave."""
    async with AsyncModbusTcpClient(item['slave_ip'], port=slave_port) as client:
        if client.protocol:
            if item['meter_type'] == "M1M20":
                return await readModbusDataM1M20(client, item['slave_ip'], node_name=item['node_name'])
            elif item['meter_type'] == "PAC3220":
                if item['node_name'] in not_connected_data:
                    not_connected_data.pop(item['node_name'])
                return await readModbusDataPAC3220(client, item['slave_ip'], node_name=item['node_name'])
        else:
            log_message(f"Failed to connect to Modbus Slave at {item['slave_ip']}, name: {item['node_name']}")
            data=takeDataFromDict(item, not_connected_data)
            if data:
                return data
            else:
                data=fetchLastData(item, conn, not_connected_data)
                return data  # Return default values on failure


        # Main loop
async def mainTCP(slave_info, conn):
    # try:
    #     await ws.connect(websocket_url, header=[f"Authorization: Bearer {token}"])
    # except Exception as e:
    #     print(f"WebSocket connection failed: {e}")



    slave_port = 502
    try:
        results = await poll_modbus_slaves(slave_info, slave_port, conn)

        # # Process results
        # for (data, timestamp) in results:
        #     if data:
        #         print(f"Received data: {data}")  # Process data as needed

        return results

    finally:
        log_message("WebSocket connection closed.")

