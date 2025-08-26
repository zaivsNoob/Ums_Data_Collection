import pyodbc
import datetime
import calendar
import os
from dotenv import load_dotenv

dotenv_path = '.env'
load_dotenv()

DATABASE_HOST = os.getenv('DATABASE_HOST')
DATABASE_NAME = os.getenv('DATABASE_NAME')
DATABASE_USER = os.getenv('DATABASE_USER')
DATABASE_PASSWORD =os.getenv('DATABASE_PASSWORD')


connection_string = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={DATABASE_HOST};DATABASE={DATABASE_NAME};UID={DATABASE_USER};PWD={DATABASE_PASSWORD}'



tables_to_delete = ['Source_Data','Shed_Data','Plant_Data','LT_Data','Inverter_Data','Water','Natural_Gas','Other_Sensor'
                    
                    ]


try:
    connection = pyodbc.connect(connection_string)

    cursor = connection.cursor()

except pyodbc.Error as e:
    print(f"Error connecting to the database: {e}")
    exit()

for table_name in tables_to_delete:
    delete_query = f"DELETE FROM {table_name} "

    try:
        cursor.execute(delete_query)
        connection.commit()
        print(f"Data deleted from {table_name} successfully.")
    except pyodbc.Error as e:
        connection.rollback()
        print(f"Error deleting data from {table_name}: {e}")    