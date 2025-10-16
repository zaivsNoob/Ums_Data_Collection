import traceback
from electricty_handlers import log_message, connect_to_database
import pyodbc
import monthly_yearly_utils 
import datetime
import calendar


def fetchDataForSensor(cursor, dataset):
    try:
        column_list= ['Node_Name', 'temperature', 'humidity', 'co2']
        cursor.execute("SELECT node_name, zlan_ip, meter_no FROM Source_Info WHERE (resource_type = 'Sensor')")
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

    except Exception as e:
        log_message(f"Error reading node for {node_name}: {traceback.format_exc()}")

def processReadNodeForSensor(current_timestamp, sensor_results, sensor_data_list, sensor_dgr_data_list):
    for data in sensor_results:
        readNode(current_timestamp, data, data['Node_Name'], sensor_data_list, sensor_dgr_data_list)


def bulkInsertForSensor(cursor, sensor_data_list, sensor_dgr_data_list):
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

    except Exception as e:
        log_message(f"Database error during bulk insert: {traceback.format_exc()} {e}")

        sensor_data_list.clear()  # Clear the list after insertion
        sensor_dgr_data_list.clear()  # Clear the list after insertion
        raise                                               

def slaveInfoSensor(cursor):
    try:
        cursor.execute("SELECT zlan_ip, meter_no, meter_model FROM Source_Info WHERE resource_type= 'Sensor' AND connection_type = 'Zlan'")
        rows = cursor.fetchall()
        slave_info_zlan={}
        for row in rows:
            zlan_ip, meter_no, meter_model = row
            if zlan_ip not in slave_info_zlan:
                slave_info_zlan[zlan_ip]={}
            slave_info_zlan[zlan_ip][meter_no]= meter_model
        return slave_info_zlan

    except Exception as e:
        log_message(f"Error in slaveInfoSensor: {traceback.format_exc()}")
        return []


if __name__ == "__main__":
    pass