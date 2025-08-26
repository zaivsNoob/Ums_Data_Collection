import asyncio
import json
import pyodbc
import redis
import struct
import websocket
from datetime import datetime
from pymodbus.client import AsyncModbusTcpClient
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Connection strings
class DatabaseConfig:
    def __init__(self):
        self.connection_string = (
            f"DRIVER={'ODBC Driver 17 for SQL Server'};"
            f"SERVER={os.getenv('DB_SERVER')};"
            f"DATABASE={os.getenv('DB_NAME')};"
            f"UID={os.getenv('DB_UID')};"
            f"PWD={os.getenv('DB_PWD')};"
        )

class RedisCache:
    def __init__(self):
        self.redis_client = redis.StrictRedis(
            host=os.getenv('REDIS_HOST'),
            port=int(os.getenv('REDIS_PORT')),
            password=os.getenv('REDIS_PASSWORD'),
            decode_responses=True
        )

    def get_or_load_meter_data(self):
        cache_key = "meter_dict_cache"
        cached_data = self.redis_client.hgetall(cache_key)

        if cached_data:
            # Convert the cached data back to a dictionary
            meter_dict = {int(key): value for key, value in cached_data.items()}
            print("Loaded meter data from Redis cache.")
        else:
            # Fetch data from the database
            conn = pyodbc.connect(DatabaseConfig().connection_string)
            cursor = conn.cursor()
            cursor.execute("SELECT meter_no, node_name FROM Source_Info WHERE (category IN ('Electricity', 'Grid', 'Solar', 'Diesel_Generator', 'Gas_Generator')) AND source_type IN ('Source', 'Load', 'Meter_Bus_Bar', 'LB_Meter')")
            rows = cursor.fetchall()
            # Create the meter_dict
            meter_dict = {row[0]: row[1] for row in rows}
            
            # Cache the meter_dict in Redis using HSET
            with self.redis_client.pipeline() as pipe:
                for meter_no, node_name in meter_dict.items():
                    pipe.hset(cache_key, meter_no, node_name)
                pipe.execute()  # Execute all commands in a single batch

            conn.close()
            print("Loaded meter data from database and cached in Redis.")

        return meter_dict

class WebSocketClient:
    def __init__(self, websocket_url, token):
        self.websocket_url = websocket_url
        self.token = token
        self.ws = websocket.WebSocket()

    def send_notification(self, node_name, power, status):
        try:
            notification_data = {
                "node_name": node_name,
                "power": power,
                "status": status
            }
            self.ws.send(json.dumps(notification_data))
            print(f"Notification sent for node: {node_name}")
        except Exception as e:
            print(f"Error sending notification: {e}")

    def connect(self):
        try:
            self.ws.connect(self.websocket_url, header=[f"Authorization: Bearer {self.token}"])
        except Exception as e:
            print(f"WebSocket connection failed: {e}")

    def close(self):
        self.ws.close()
        print("WebSocket connection closed.")

class ModbusDataProcessor:
    @staticmethod
    def convert_int16_to_32_float(registers, byteorder=Endian.Big, wordorder=Endian.Big):
        decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=byteorder, wordorder=wordorder)
        return decoder.decode_32bit_float()

    @staticmethod
    def convert_int16_to_64_float(registers, byteorder=Endian.Big, wordorder=Endian.Big):
        decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=byteorder, wordorder=wordorder)
        return decoder.decode_64bit_float()

class DataInserter:
    def __init__(self, connection_string):
        self.connection_string = connection_string

    def bulk_insert_data(self, data_list):
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()

            insert_query = """
            INSERT INTO dbo.Source_Data_New (
                node_name, timestamp, active_power, pv_l1, pv_l2, pv_l3, 
                current_l1, current_l2, current_l3, frequency, net_energy, status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            records = [
                (
                    entry["node_name"],
                    entry["timestamp"],
                    entry["active_power"],
                    entry["pv_l1"],
                    entry["pv_l2"],
                    entry["pv_l3"],
                    entry["current_l1"],
                    entry["current_l2"],
                    entry["current_l3"],
                    entry["frequency"],
                    entry["net_energy"],
                    entry["status"]
                )
                for entry in data_list
            ]

            cursor.fast_executemany = True
            cursor.executemany(insert_query, records)
            conn.commit()
            print(f"Inserted {len(records)} records successfully at {datetime.now()}")
        except Exception as e:
            print(f"Bulk insert failed: {e}")
        finally:
            if conn:
                conn.close()

class MeterDataCollector:
    def __init__(self, meter_dict, modbus_client, data_inserter, websocket_client):
        self.meter_dict = meter_dict
        self.modbus_client = modbus_client
        self.data_inserter = data_inserter
        self.websocket_client = websocket_client

    async def collect_and_insert_data(self):
        data = []
        try:
            addresses = [0, 120, 240, 360, 480]
            meter_no = 1

            for addr in addresses:
                try:
                    response = await self.modbus_client.read_holding_registers(slave=1, address=addr, count=120)
                except Exception as e:
                    print(f"Error reading Modbus registers at address {addr}: {e}")

                current_time = datetime.now()
                formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

                if response and not response.isError():
                    registers = response.registers

                    for i in range(3):
                        data.append({
                            "node_name": self.meter_dict[meter_no],
                            "timestamp": formatted_time,
                            "active_power": ModbusDataProcessor.convert_int16_to_32_float(registers[14 + i * 40:16 + i * 40]) * 0.001,
                            "pv_l1": ModbusDataProcessor.convert_int16_to_32_float(registers[0 + i * 40:2 + i * 40]),
                            "pv_l2": ModbusDataProcessor.convert_int16_to_32_float(registers[2 + i * 40:4 + i * 40]),
                            "pv_l3": ModbusDataProcessor.convert_int16_to_32_float(registers[4 + i * 40:6 + i * 40]),
                            "current_l1": ModbusDataProcessor.convert_int16_to_32_float(registers[6 + i * 40:8 + i * 40]),
                            "current_l2": ModbusDataProcessor.convert_int16_to_32_float(registers[8 + i * 40:10 + i * 40]),
                            "current_l3": ModbusDataProcessor.convert_int16_to_32_float(registers[10 + i * 40:12 + i * 40]),
                            "frequency": ModbusDataProcessor.convert_int16_to_32_float(registers[12 + i * 40:14 + i * 40]),
                            "net_energy": ModbusDataProcessor.convert_int16_to_64_float(registers[20 + i * 40:24 + i * 40]) / 1000
                                         - ModbusDataProcessor.convert_int16_to_64_float(registers[24 + i * 40:28 + i * 40]) / 1000,
                            "status": True if (ModbusDataProcessor.convert_int16_to_32_float(registers[14 + i * 40:16 + i * 40]) * 0.001 > 0) else False
                        })
                        meter_no += 1

            self.data_inserter.bulk_insert_data(data)

        except Exception as e:
            print(f"Error collecting or inserting data: {e}")

# Main loop
async def main():
    try:
        websocket_client = WebSocketClient(websocket_url="ws://192.168.68.61:8086/ws/test/", token="4d07820f27d23a02eca124d5287e7e8eb32cc550")
        websocket_client.connect()
        print("signal")
        redis_cache = RedisCache()
        meter_dict = redis_cache.get_or_load_meter_data()

        data_inserter = DataInserter(DatabaseConfig().connection_string)

        while True:
            slave_info = ['192.168.68.241','192.168.68.241']

            for slave in slave_info:
                async with AsyncModbusTcpClient(slave, port=502) as modbus_client:
                    if modbus_client:
                        meter_data_collector = MeterDataCollector(meter_dict, modbus_client, data_inserter, websocket_client)
                        await meter_data_collector.collect_and_insert_data()



            await asyncio.sleep(5)

    except Exception as e:
        print(f"Main loop error: {e}")

asyncio.run(main())


