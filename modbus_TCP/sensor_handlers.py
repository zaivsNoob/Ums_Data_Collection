import traceback
from electricty_handlers import log_message, connect_to_database
import pyodbc
import monthly_yearly_utils 
import datetime
import calendar


def fetchDataForSensor(cursor, dataset):
    try:
        column_list= ['Node_Name', 'temperature', 'humidity', 'co2']
        cursor.execute("SELECT node_name, zlan_ip, meter_no FROM Source_Info WHERE (category = 'Other_Sensor')")
        rows = cursor.fetchall()
        results = []


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
                    if column_list[i] != 'Node_Name':
                        temp[column_list[i]]=data_for_node[f'data_{i}']
            results.append(temp)
        
        return results
    except Exception as e:
        log_message(f"Error in fetchDataForSensor: {traceback.format_exc()}")
        return []
    
def readNode(current_timestamp, temp, node_name, sensor_data_list, sensor_dgr_data_list):

    try:             
            
        sensor_data_list.append((current_timestamp, temp['Node_Name'], temp['temperature'], temp['humidity'], temp['co2']))

        sensor_dgr_data_list.append((current_timestamp, temp['Node_Name'], temp['temperature'], temp['humidity'], temp['co2']))

        if (current_timestamp.hour == 23 and current_timestamp.minute == 59) or (current_timestamp.hour == 0 and current_timestamp.minute <= 5):
            pass
            # last_update_time = energy_store[node_name].get('last_update')

            # If last_update doesn't exist or last update was NOT from previous day's 23:59
            # if (current_timestamp.hour == 23 and current_timestamp.minute == 59) or (last_update_time.hour == 23 and last_update_time.minute == 0) or (last_update_time.hour != 23 and last_update_time.minute != 59):
            #     log_message(f'Midnight reset at: {current_timestamp}')
                # Perform the reset operations



    except Exception as e:
        log_message(f"Error reading node for {node_name}: {traceback.format_exc()}")

def processReadNodeForSensor(current_timestamp, sensor_results, sensor_data_list, sensor_dgr_data_list):
    for data in sensor_results:
        readNode(current_timestamp, data, data['Node_Name'], sensor_data_list, sensor_dgr_data_list)


def bulkInsertForSensor(cursor, sensor_data_list, sensor_dgr_data_list, DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD):
    try:
        
        if sensor_data_list:
            cursor.executemany('''
                INSERT INTO Other_Sensor (timedate, node, temperature, humidity, co2)
                VALUES (?, ?, ?, ?, ?)
            ''', sensor_data_list)
            sensor_data_list.clear()  # Clear the list after insertion

        if sensor_dgr_data_list:
            cursor.executemany('''
                INSERT INTO Other_Sensor_DGR (timedate, node, temperature, humidity, co2)
                VALUES (?, ?, ?, ?, ?)
            ''', sensor_dgr_data_list)
            sensor_dgr_data_list.clear()  # Clear the list after insertion

    except pyodbc.Error as e:
        log_message(f"Database error during bulk insert: {traceback.format_exc()} {sensor_data_list}")
        conn = connect_to_database(DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)  # Attempt to reconnect
        cursor = conn.cursor() if conn else None

        sensor_data_list.clear()  # Clear the list after insertion
        sensor_dgr_data_list.clear()  # Clear the list after insertion                                               
    except Exception as e:
        log_message(f"Unexpected error during bulk insert: {traceback.format_exc()}")


if __name__ == "__main__":
    pass