import redis
import traceback
from redis.exceptions import ConnectionError


# create connection to redis

class RedisWrapper(object):
    shared_state = {}
    state_key_format = "%s-%s-%s"

    def _get_connection(self, host, port, db):
        redis_state_key = self.state_key_format.format(host, port, db)
        if redis_state_key not in self.shared_state:
            connection = self.setup_connection(host, port, db)
            self.shared_state[redis_state_key] = connection
        return self.shared_state[redis_state_key]

    def setup_connection(self, host, port, db):
        print("Connection creating for %s , %s, %s" % (host, port, db))
        try:
            connection_pool = redis.ConnectionPool(host=host, port=port, db=db)
            return redis.StrictRedis(connection_pool=connection_pool)
        except ConnectionError:
            print(traceback.format_exc())
            print("Got connection error.")

    def get_connection(self, host, port, db):
        return self._get_connection(host, port, db)


redis_wrap = RedisWrapper()
get_redis_conn = redis_wrap.get_connection
