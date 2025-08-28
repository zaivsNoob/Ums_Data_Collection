# Description: This script reads data from Modbus devices connected to ZLAN 5143 Ethernet Gateway and inserts the data into a Microsoft SQL Server database.    
import asyncio
import struct
from pymodbus.client import AsyncModbusTcpClient
from datetime import datetime

from dotenv import load_dotenv
import os

from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian


load_dotenv()

log_file_path = os.getenv('LOG_FILE_PATH')

def log_message(message):
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"{datetime.now()}: {message}\n")

# connection_string = (
#     f"DRIVER={'ODBC Driver 17 for SQL Server'};"
#     f"SERVER={os.getenv('DB_SERVER')};"
#     f"DATABASE={os.getenv('DB_NAME')};"
#     f"UID={os.getenv('DB_UID')};"
#     f"PWD={os.getenv('DB_PWD')};"
# )

# Retry function wrapper
async def retry_on_failure(func, retries=5, delay=2, *args, **kwargs):
    for attempt in range(1, retries + 1):
        try:
            return await func(*args, **kwargs)
        except asyncio.TimeoutError:
            log_message(f"Timeout error occurred. Retry {attempt} of {retries}...")
            if attempt < retries:
                await asyncio.sleep(delay)
            else:
                log_message("Maximum retries reached. Giving up.")
                raise
        except Exception as e:
            log_message(f"Unexpected error: {e}. Retry {attempt} of {retries}...")
            if attempt < retries:
                await asyncio.sleep(delay)
            else:
                log_message("Maximum retries reached. Giving up.")
                raise

def convertRegistersToDataM1M20_4(register1, register2, register3, register4, resolution):
    combined_value = int(f"{register1:04x}{register2:04x}{register3:04x}{register4:04x}", 16)
    return round(combined_value * resolution, 3)

def convertRegistersToDataM1M20_2(register1, register2, resolution):
    combined_value = int((hex(register1) + hex(register2)[2:]), 16)
    return round(combined_value * resolution,3)




def convert_int16_to_32_int(registers, byteorder, wordorder=Endian.Big):
    decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=byteorder, wordorder=wordorder)
    return decoder.decode_32bit_int()

def convert_int16_to_64_float(registers, byteorder=Endian.Big, wordorder=Endian.Big):
    decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=byteorder, wordorder=wordorder)
    return decoder.decode_64bit_float()

def convert_int16_to_32_float(registers, byteorder='little'):
    # Check if at least two registers are provided
    if len(registers) < 2:
        raise ValueError("At least two registers are required for 32-bit float conversion")
    
    # Convert 16-bit registers to bytes in little-endian order
    byte_data = bytes([
        (registers[0] & 0xFF),          # Low byte of first register
        ((registers[0] >> 8) & 0xFF),   # High byte of first register
        (registers[1] & 0xFF),          # Low byte of second register
        ((registers[1] >> 8) & 0xFF)    # High byte of second register
    ])
    
    # Unpack bytes to float (little-endian)
    return struct.unpack('<f', byte_data)[0]


# def convert_int16_to_32_float(registers, byteorder=Endian.Little, wordorder=Endian.Little):
#     # Create decoder with the specified byte and word order
#     decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=byteorder, wordorder=wordorder)
#     return decoder.decode_32bit_float()

async def readModbusZLAN(client, data_fetch_config, slave_ip):

    data = []
    try:

        # response = await retry_on_failure(client.read_holding_registers, retries=3, delay=2, address=40, count=20, slave=1)
        # registers=response.registers
        # print(convert_int16_to_32_int(registers[2:4]))



        meter_no=1
        for config in data_fetch_config:
            response = await retry_on_failure(client.read_holding_registers, retries=3, delay=2, address=config["start_reg"], count=config["count"], slave=1)



            if not response.isError():
                registers = response.registers
                # print(f"registers for {slave_ip}==={registers}")

                # for i in range(config["meter_fetched"]):
                #     data.append({
                #         "Node_Name": meter_dict[meter_no],
                #         "Net_Energy": convert_int16_to_32_float(registers[0 + i * 20:2 + i * 20]),
                #         "Voltage1": convert_int16_to_32_float(registers[2 + i * 20:4 + i * 20]),
                #         "Voltage2": convert_int16_to_32_float(registers[4 + i * 20:6 + i * 20]),
                #         "Voltage3": convert_int16_to_32_float(registers[6 + i * 20:8 + i * 20]),
                #         "Current1": convert_int16_to_32_float(registers[8 + i * 20:10 + i * 20]),
                #         "Current2": convert_int16_to_32_float(registers[10 + i * 20:12 + i * 20]),
                #         "Current3": convert_int16_to_32_float(registers[12 + i * 20:14 + i * 20]),
                #         "Power": convert_int16_to_32_float(registers[14 + i * 20:16 + i * 20]),
                #         "Frequency": convert_int16_to_32_float(registers[16 + i * 20:18 + i * 20]),
                #         "Reactive_Energy": 0,
                #         "Status": 0 if convert_int16_to_32_float(registers[14 + i * 20:16 + i * 20]) == 0 else 1,
                #     })
                
                for i in range(config["meter_fetched"]):
                    data_entry = {}
                    
                    for j in range(20):
                        start_index = j * 2 + i * 40
                        end_index = start_index + 2

                        data_entry[f"data_{j + 1}"] = convert_int16_to_32_float(registers[start_index:end_index])
                    meter_no+=1
                    # print(meter_no)
                    

                    data.append(data_entry)

 
        # print(data)        



                    

    except asyncio.TimeoutError:
        log_message(f"Failed to retrieve data from {client.host} after retries.")

    except Exception as e:
        log_message(f"Unexpected failure: {e}")


    return data

# async def readModbusZLANStorage(client, data_fetch_config):

#     data = []
#     try:
#         for config in data_fetch_config:
#             response = await retry_on_failure(client.read_holding_registers, retries=3, delay=2, address=config["start_reg"], count=config["count"], slave=1)

#             if not response.isError():
#                 registers = response.registers

#                 for i in range(config["meter_fetched"]):
#                     data_entry = {}

#                     for j in range(60):  # Adjusted for 60 data points per meter
#                         start_index = j * 2 + i * 120
#                         end_index = start_index + 2
#                         data_entry[f"data_{j + 1}"] = convert_int16_to_32_float(registers[start_index:end_index])

#                     data.append(data_entry)

#     except asyncio.TimeoutError:
#         log_message(f"Failed to retrieve data from {client.host} after retries.")

#     except Exception as e:
#         log_message(f"Unexpected failure: {e}")

#     client.close()
#     return data
def generate_slave_config(slave):
                total_meters = slave['number_of_meters']  # Total number of meters for this slave
                meters_per_config = 3  # Number of meters per configuration block

                slave_data_fetch_config = []
                full_blocks = total_meters // meters_per_config  # Number of full blocks with 3 meters
                remaining_meters = total_meters % meters_per_config  # Remaining meters after full blocks

                for i in range(full_blocks):
                    slave_data_fetch_config.append({
                        "start_reg": i * 120,
                        "count": 120,
                        "meter_fetched": meters_per_config
                    })

                if remaining_meters > 0:
                    slave_data_fetch_config.append({
                        "start_reg": full_blocks * 120,
                        "count": remaining_meters * 40,
                        "meter_fetched": remaining_meters
                    })

                # slave_config_storage = [
                #     {
                #         "start_reg": 600 + i * 120,
                #         "count": 120,
                #         "meter_fetched": 1
                #     } for i in range(total_meters)
                # ]

                return slave_data_fetch_config

# Main loop
async def mainZLAN(slave_info):
    try:
            # print(slave_info)



            # Replace the placeholder with the following code
            updated_slave_info = []
            for slave in slave_info:
                data_fetch_config= generate_slave_config(slave)
                updated_slave_info.append({
                    "slave_ip": slave['slave_ip'],
                    "addresses": data_fetch_config,

                })

            slave_info = updated_slave_info
            # print(slave_info) 
            slave_port = 502   
            result={}
            result_storage={}
            for item in slave_info:

                log_message(f"Connecting to Modbus ZLAN Slave at {item['slave_ip']}")

                async with AsyncModbusTcpClient(item['slave_ip'], port=slave_port) as client:
    
                    if client:
                        

                        data  = await readModbusZLAN(client, item['addresses'], item['slave_ip'])
                        # storage_data=await readModbusZLANStorage(client, item['addresses_storage'])

                        result[item['slave_ip']]=(data)
                        # result_storage[item['slave_ip']]=(storage_data)

                    else:
                        data = []
                        # storage_data=[]
                        log_message(f"Failed to connect to Modbus ZLAN Slave at {item['slave_ip']}")
              
            return result, result_storage 

    except Exception as e:
        log_message(f"Database query failed: {e}")

    finally:
        log_message("Data fetched from ZLAN successfully.")
