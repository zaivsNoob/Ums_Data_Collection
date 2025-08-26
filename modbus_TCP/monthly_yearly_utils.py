import pyodbc
import calendar

def connect_to_database(host, database, user, password, path):
    try:
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={database};UID={user};PWD={password}"
        conn = pyodbc.connect(conn_str)
        log_message("Database connection established.", path)
        return conn
    except Exception as e:
        log_message(f"Database connection error: {e}", path)
        return None

def get_last_entry_month(cursor):
    cursor.execute("SELECT TOP 1 date FROM Yearly_Total_Energy ORDER BY date DESC")
    return cursor.fetchone()

def get_last_entry_month_water(cursor):
    cursor.execute("SELECT TOP 1 date FROM Yearly_Water ORDER BY date DESC")
    return cursor.fetchone()

def get_last_entry_month_gas(cursor):
    cursor.execute("SELECT TOP 1 date FROM Yearly_Natural_Gas ORDER BY date DESC")
    return cursor.fetchone()

# def get_source_info(cursor):
#     cursor.execute("SELECT node_name,Source_type FROM Source_Info WHERE Source_type='Load' OR Source_type='Source' OR Source_type='Load' OR Source_type='Bus_Bar'")
#     return [item for sublist in cursor.fetchall() for item in sublist]

def get_source_info(cursor):
    query = """
    SELECT node_name
    FROM Source_Info 
    WHERE resource_type= 'Electricity'
    """
    cursor.execute(query)
    rows=cursor.fetchall()
    node_names = [row[0] for row in rows]
    return node_names

def get_water_info(cursor):
    query = """
    SELECT node_name 
    FROM Source_Info 
    WHERE resource_type= 'Water'
    """
    cursor.execute(query)
    rows=cursor.fetchall()
    node_names = [row[0] for row in rows]
    return node_names

def get_gas_info(cursor):
    query = """
    SELECT node_name 
    FROM Source_Info 
    WHERE resource_type IN ('Natural_Gas', 'Steam')
    """
    cursor.execute(query)
    rows=cursor.fetchall()
    node_names = [row[0] for row in rows]
    return node_names

def get_node_array(cursor, last_entry_month):
    if last_entry_month:
        cursor.execute("SELECT node FROM Yearly_Total_Energy WHERE MONTH(date) = ? AND YEAR(date) = ?", 
                       (last_entry_month[0].month, last_entry_month[0].year))
        return [item for sublist in cursor.fetchall() for item in sublist]
    else:
        return []
    

def get_water_array(cursor, last_entry_month):
    if last_entry_month:
        cursor.execute("SELECT node FROM Yearly_Water WHERE MONTH(date) = ? AND YEAR(date) = ?", 
                       (last_entry_month[0].month, last_entry_month[0].year))
        return [item for sublist in cursor.fetchall() for item in sublist]
    else:
        return []
    
def get_gas_array(cursor, last_entry_month):
    if last_entry_month:
        cursor.execute("SELECT node FROM Yearly_Natural_Gas WHERE MONTH(date) = ? AND YEAR(date) = ?", 
                       (last_entry_month[0].month, last_entry_month[0].year))
        return [item for sublist in cursor.fetchall() for item in sublist]
    else:
        return []

def get_yearly_data(cursor, source, current_month):
    # Fetch energy, cost, energy_mod, and cost_mod
    cursor.execute("""
        SELECT COALESCE(SUM(energy), 0), 
               COALESCE(SUM(cost), 0), 
               COALESCE(SUM(energy_mod), 0), 
               COALESCE(SUM(cost_mod), 0),
                COALESCE(SUM(runtime), 0) 
        FROM Monthly_Total_Energy 
        WHERE node = ? AND MONTH(date) = ?""", 
        (source, current_month))
    result = cursor.fetchone()
    energy = result[0]
    cost = result[1]
    energy_mod = result[2]
    cost_mod = result[3]
    runtime=result[4]
    
    # Fetch total power cut duration
    cursor.execute("""
        SELECT 
            COALESCE(SUM(duration_in_min), 0) AS total_duration, 
            COUNT(*) AS power_cut_count
        FROM Power_Cut
        WHERE generator_name = ? AND MONTH(start_time) = ?
    """, (source, current_month))

    result = cursor.fetchone()
    power_cut = result[0]  # Total duration of power cuts
    count_powercut = result[1]  # Count of power cuts

    return energy, cost, power_cut, count_powercut, energy_mod, cost_mod, runtime


def get_yearly_water_data(cursor, source, current_month):
    cursor.execute("SELECT SUM(sensor_value), SUM(sensor_cost), SUM(runtime), SUM(volume) FROM Monthly_Water WHERE node = ? AND MONTH(date) = ?", 
                   (source, current_month))
    result = cursor.fetchone()
    sensor_value=result[0]
    sensor_cost=result[1]
    runtime=result[2]
    volume=result[3]


    return sensor_value, sensor_cost, runtime, volume


def get_yearly_gas_data(cursor, source, current_month):
    cursor.execute("SELECT SUM(sensor_value), SUM(sensor_cost), SUM(runtime), SUM(volume) FROM Monthly_Natural_Gas WHERE node = ? AND MONTH(date) = ?", 
                   (source, current_month))
    result = cursor.fetchone()
    sensor_value=result[0]
    sensor_cost=result[1]
    runtime=result[2]
    volume=result[3]


    return sensor_value, sensor_cost, runtime, volume

def get_energy_and_cost_monthly(cursor, source, current_date):
    # Query to get the current day's data
    cursor.execute("""
        SELECT COALESCE(today_energy, 0.0), 
               COALESCE(cost, 0.0), 
               COALESCE(ABS(today_energy), 0.0), 
               COALESCE(cost_mod, 0.0)
        FROM Source_Data
        WHERE node = ? AND CAST(timedate AS DATE) = ?
        ORDER BY timedate DESC
    """, (source, current_date))
    result = cursor.fetchone()

    if result is None:
        return
    else:
        # Unpack current day's data
        energy = result[0]
        cost = result[1]
        energy_mod = result[2]
        cost_mod = result[3]


    return energy, cost, energy_mod, cost_mod

def get_water_data_monthly(cursor, source, current_date):

    # Query to get the current day's data
    cursor.execute("""
        SELECT COALESCE(sensor_value, 0.0), 
               COALESCE(sensor_cost, 0.0),  
               COALESCE(today_volume, 0.0)
        FROM Water
        WHERE node = ? AND CAST(timedate AS DATE) = ?
        ORDER BY timedate DESC
    """, (source, current_date))
    result = cursor.fetchone()

    if result is None:
        return 0.0, 0.0, 0.0
    else:
        # Unpack current day's data
        sensor_value = result[0]
        total_cost = result[1]
        volume= result[2] 

    return sensor_value, total_cost, volume

def get_gas_data_monthly(cursor, source, current_date):
    # Query to get the current day's data
    cursor.execute("""
        SELECT COALESCE(sensor_value, 0.0), 
               COALESCE(sensor_cost, 0.0),  
               COALESCE(today_volume, 0.0)
        FROM Natural_Gas
        WHERE node = ? AND CAST(timedate AS DATE) = ?
        ORDER BY timedate DESC
    """, (source, current_date))
    result = cursor.fetchone()

    if result is None:
        return 0.0, 0.0, 0.0
    else:
        # Unpack current day's data
        sensor_value = result[0]
        total_cost = result[1]
        volume= result[2] 

    return sensor_value, total_cost, volume





# def get_runtime_monthly(cursor, source, current_date):
#     cursor.execute("SELECT COUNT(*) FROM Source_Data WHERE power > 0 AND node = ? AND CAST(timedate AS DATE) = ?", 
#                    (source, current_date))
#     runtime_result = cursor.fetchone()[0]
    
#     return runtime_result

def get_runtime_monthly(cursor, source, current_date):
    cursor.execute("""
        SELECT COUNT(DISTINCT FORMAT(timedate, 'yyyy-MM-dd HH:mm'))
        FROM Source_Data
        WHERE power > 0
          AND node = ?
          AND CAST(timedate AS DATE) = ?
    """, (source, current_date))
    
    runtime_result = cursor.fetchone()[0]
    
    return runtime_result


def get_water_runtime_monthly(cursor, source, current_date):
    cursor.execute("""
        SELECT COUNT(DISTINCT FORMAT(timedate, 'yyyy-MM-dd HH:mm'))
        FROM Water
        WHERE sensor_value > 0
          AND node = ?
          AND CAST(timedate AS DATE) = ?
    """, (source, current_date))
    
    runtime_result = cursor.fetchone()[0]
    
    return runtime_result

def get_gas_runtime_monthly(cursor, source, current_date):
    cursor.execute("""
        SELECT COUNT(DISTINCT FORMAT(timedate, 'yyyy-MM-dd HH:mm'))
        FROM Natural_Gas
        WHERE sensor_value > 0
          AND node = ?
          AND CAST(timedate AS DATE) = ?
    """, (source, current_date))
    
    runtime_result = cursor.fetchone()[0]
    
    return runtime_result


def check_existing_record(cursor, current_date, source):
    check_existing_query = "SELECT COUNT(*) FROM Monthly_Total_Energy WHERE date = ? AND node = ?"
    cursor.execute(check_existing_query, (current_date, source))
    return cursor.fetchone()[0]

def check_existing_water_record(cursor, current_date, source):
    check_existing_query = "SELECT COUNT(*) FROM Monthly_Water WHERE date = ? AND node = ?"
    cursor.execute(check_existing_query, (current_date, source))
    return cursor.fetchone()[0]

def check_existing_gas_record(cursor, current_date, source):
    check_existing_query = "SELECT COUNT(*) FROM Monthly_Natural_Gas WHERE date = ? AND node = ?"
    cursor.execute(check_existing_query, (current_date, source))
    return cursor.fetchone()[0]

def update_or_insert_record(cursor, node, energy_result, cost, powercut, count_powercut, energy_mod, cost_mod, runtime, last_day_of_current_month, 
                                                        last_entry_month, current_month_number, node_array):
    if node in node_array and last_entry_month and last_entry_month[0].month == current_month_number:
        cursor.execute("UPDATE Yearly_Total_Energy SET energy = ?, cost = ? , powercut_in_min=?, count_powercuts=?, energy_mod=?, cost_mod=?, runtime=? WHERE MONTH(date) = ? AND YEAR(date) = ? AND node = ?",
                       (energy_result, cost, powercut, count_powercut, energy_mod, cost_mod, runtime, last_entry_month[0].month, last_entry_month[0].year, node))
    else:
        cursor.execute("INSERT INTO Yearly_Total_Energy (date, node, energy, cost, powercut_in_min, count_powercuts, energy_mod, cost_mod, runtime) VALUES (?, ?, ?, ?,?,?, ?, ?, ?)",
                       (last_day_of_current_month, node, energy_result, cost, powercut,count_powercut, energy_mod, cost_mod, runtime))
        

def update_or_insert_water_record(cursor, source, sensor_value, sensor_cost, runtime, volume, last_day_of_current_month, last_entry_month, current_month_number, water_array):
    if source in water_array and last_entry_month and last_entry_month[0].month == current_month_number:
        cursor.execute("UPDATE Yearly_Water SET sensor_value = ?, sensor_cost = ?, runtime=?, volume=? WHERE MONTH(date) = ? AND YEAR(date) = ? AND node = ?",
                       (sensor_value, sensor_cost, runtime, volume, last_entry_month[0].month, last_entry_month[0].year, source))
    else:
        cursor.execute("INSERT INTO Yearly_Water (date, node, sensor_value, sensor_cost, runtime, volume) VALUES (?, ?, ?, ?, ?, ?)",
                       (last_day_of_current_month, source, sensor_value, sensor_cost, runtime, volume))
        
def update_or_insert_gas_record(cursor, source, sensor_value, sensor_cost, runtime, volume, last_day_of_current_month, last_entry_month, current_month_number, gas_array):
    if source in gas_array and last_entry_month and last_entry_month[0].month == current_month_number:
        cursor.execute("UPDATE Yearly_Natural_Gas SET sensor_value = ?, sensor_cost = ?, runtime=?, volume=? WHERE MONTH(date) = ? AND YEAR(date) = ? AND node = ?",
                       (sensor_value, sensor_cost, runtime, volume, last_entry_month[0].month, last_entry_month[0].year, source))
    else:
        cursor.execute("INSERT INTO Yearly_Natural_Gas (date, node, sensor_value, sensor_cost, runtime, volume) VALUES (?, ?, ?, ?, ?, ?)",
                       (last_day_of_current_month, source, sensor_value, sensor_cost, runtime, volume))

def update_record_monthly(cursor, energy_result, cost, runtime,energy_mod, cost_mod, current_date, node):
    update_query = "UPDATE Monthly_Total_Energy SET energy = ?, cost=? , runtime =?, energy_mod=?, cost_mod=? WHERE date = ? AND node = ? " 
    cursor.execute(update_query, (energy_result, cost, runtime, energy_mod, cost_mod, current_date, node))


def update_water_record_monthly(cursor, node, sensor_value, sensor_cost, runtime, volume,  current_date):
    update_query = "UPDATE Monthly_Water SET sensor_value = ?, sensor_cost=?, runtime=?, volume= ? WHERE date = ? AND node = ? " 
    cursor.execute(update_query, (sensor_value, sensor_cost, runtime, volume, current_date, node))

def update_gas_record_monthly(cursor, node, sensor_value, sensor_cost, runtime, volume,  current_date):
    update_query = "UPDATE Monthly_Natural_Gas SET sensor_value = ?, sensor_cost=?, runtime=?, volume= ? WHERE date = ? AND node = ? " 
    cursor.execute(update_query, (sensor_value, sensor_cost, runtime, volume, current_date, node))


def insert_record_monthly(cursor, energy_result, cost, runtime,energy_mod, cost_mod, current_date, node):
    insert_query = "INSERT INTO Monthly_Total_Energy (date, node, energy, cost, runtime, energy_mod, cost_mod) VALUES (?, ?, ?, ?, ?, ?, ?)"
    cursor.execute(insert_query, (current_date, node, energy_result, cost, runtime, energy_mod, cost_mod))

def insert_water_record_monthly(cursor, node, sensor_value, sensor_cost, runtime, volume, current_date):
    insert_query = "INSERT INTO Monthly_Water (date, node, sensor_value, sensor_cost, runtime, volume) VALUES (?, ?, ?, ?, ?, ?)"
    cursor.execute(insert_query, (current_date, node, sensor_value, sensor_cost, runtime, volume))

def insert_gas_record_monthly(cursor, node, sensor_value, sensor_cost, runtime, volume, current_date):
    insert_query = "INSERT INTO Monthly_Natural_Gas (date, node, sensor_value, sensor_cost, runtime, volume) VALUES (?, ?, ?, ?, ?, ?)"
    cursor.execute(insert_query, (current_date, node, sensor_value, sensor_cost, runtime, volume))

def log_operation(path, text, time):
    with open(path, 'a') as log_file:
        log_file.write(f"{text} {time}\n")
def log_message(text, path):
    with open(path, 'a') as log_file:
        log_file.write(f"{text}\n")