
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

if __name__ == "__main__":

    conn = None
    conn = electricty_handlers.connect_to_database(DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)

    while conn is None:
        conn = electricty_handlers.connect_to_database(DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
        if conn is None:
            sleep(5)

    cursor = conn.cursor()      
    try:
        while True:
            try:
                # Attempt to establish a database connection if not already connected
                if conn is None:
                    conn = electricty_handlers.connect_to_database(DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)
                    if conn is None:
                        sleep(5)
                        continue
                    cursor = conn.cursor()

                current_timestamp = datetime.datetime.now()

                # # All Table Monthly Yeary data
                electricty_handlers.electricityMonthlyInsertion(cursor, current_timestamp)
                electricty_handlers.electrticityYearlyInsertion(cursor, current_timestamp)
                water_handlers.waterMonthlyInsertion(cursor, current_timestamp)
                water_handlers.waterYearlyInsertion(cursor, current_timestamp)
                steam_handlers.steamMonthlyInsertion(cursor, current_timestamp)
                steam_handlers.steamYearlyInsertion(cursor, current_timestamp)


                # Commit data to the database
                conn.commit()
                log_message(f"Data sent to database at {current_timestamp}")
                # Pause before the next iteration
                sleep(300)
                
            except Exception as e:
                # Log the error and reset connections so they reconnect in the next loop
                log_message(f"Connection Error: {traceback.format_exc()}")
                conn = None
                client = None

    finally:
        if conn is not None:
            try:
                conn.close()
                log_message("Database connection closed.")
            except Exception as e:
                log_message(f"Error while closing database connection: {traceback.format_exc()}")
