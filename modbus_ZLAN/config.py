import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DatabaseConfig:
    def __init__(self):
        self.connection_string = (
            f"DRIVER={'ODBC Driver 17 for SQL Server'};"
            f"SERVER={os.getenv('DB_SERVER')};"
            f"DATABASE={os.getenv('DB_NAME')};"
            f"UID={os.getenv('DB_UID')};"
            f"PWD={os.getenv('DB_PWD')};"
        )

class RedisConfig:
    def __init__(self):
        self.host = os.getenv('REDIS_HOST')
        self.port = int(os.getenv('REDIS_PORT'))
        self.password = os.getenv('REDIS_PASSWORD')
