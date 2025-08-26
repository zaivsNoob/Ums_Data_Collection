import pyodbc
from datetime import datetime
from config import DatabaseConfig

class DataInserter:
    def __init__(self):
        self.connection_string = DatabaseConfig().connection_string

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
