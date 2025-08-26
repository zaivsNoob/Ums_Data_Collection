import asyncio
from datetime import datetime
from pymodbus.client import AsyncModbusTcpClient
from modbus_processor import ModbusDataProcessor

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

                        self.websocket_client.send_notification(data[-1]["node_name"], data[-1]["active_power"], data[-1]["status"])

            self.data_inserter.bulk_insert_data(data)


        except Exception as e:
            print(f"Error collecting or inserting data: {e}")