import asyncio
from redis_cache import RedisCache
from websocket_client import WebSocketClient
from data_inserter import DataInserter
from meter_data_collector import MeterDataCollector
from pymodbus.client import AsyncModbusTcpClient

async def main():
    try:
        websocket_client = WebSocketClient(websocket_url="ws://192.168.68.61:8086/ws/test/", token="02b780d3f9febcbf2dd419ed98a415e282d33f7f")
        websocket_client.connect()


        redis_cache = RedisCache()
        meter_dict = redis_cache.get_or_load_meter_data()
        data_inserter = DataInserter()

        while True:
            slave_info = ['192.168.68.241', '192.168.68.241']
            for slave in slave_info:
                async with AsyncModbusTcpClient(slave, port=502) as modbus_client:
                    meter_data_collector = MeterDataCollector(meter_dict, modbus_client, data_inserter, websocket_client)
                    await meter_data_collector.collect_and_insert_data()

            await asyncio.sleep(5)

    except Exception as e:
        print(f"Main loop error: {e}")

asyncio.run(main())
