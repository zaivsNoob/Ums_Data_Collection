import redis
from config import RedisConfig
from database import get_db_connection

class RedisCache:
    def __init__(self):
        config = RedisConfig()
        self.redis_client = redis.StrictRedis(
            host=config.host,
            port=config.port,
            password=config.password,
            decode_responses=True
        )

    def get_or_load_meter_data(self):
        cache_key = "meter_dict_cache"
        cached_data = self.redis_client.hgetall(cache_key)

        if cached_data:
            meter_dict = {int(key): value for key, value in cached_data.items()}
            print("Loaded meter data from Redis cache.")
        else:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT meter_no, node_name FROM Source_Info WHERE (category IN ('Electricity', 'Grid', 'Solar', 'Diesel_Generator', 'Gas_Generator')) AND source_type IN ('Source', 'Load', 'Meter_Bus_Bar', 'LB_Meter')")
            rows = cursor.fetchall()

            meter_dict = {row[0]: row[1] for row in rows}

            with self.redis_client.pipeline() as pipe:
                for meter_no, node_name in meter_dict.items():
                    pipe.hset(cache_key, meter_no, node_name)
                pipe.execute()

            conn.close()
            print("Loaded meter data from database and cached in Redis.")

        return meter_dict
