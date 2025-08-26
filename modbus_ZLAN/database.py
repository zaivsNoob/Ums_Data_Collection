import pyodbc
from config import DatabaseConfig

def get_db_connection():
    return pyodbc.connect(DatabaseConfig().connection_string)
