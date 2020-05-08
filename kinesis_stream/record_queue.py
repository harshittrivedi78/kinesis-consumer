import json
import traceback
from kinesis_stream.json_encoder import JSONMessageEncoder

try:
    import Queue
except ImportError:
    import queue as Queue

NAME = "records"


def get_queue_name(stream_name):
    return stream_name + '-' + NAME


class RecordQueue:
    METHOD = "redis-cache"
    redis_conn = None

    def __init__(self, stream_name):
        self._set_queue_name(stream_name)

    def _set_queue_name(self, stream_name):
        self.name = get_queue_name(stream_name)

    def put(self, data):
        shard_id, resp = data
        resp.update({"ShardId": shard_id})
        resp = json.dumps(
            resp,
            sort_keys=True,
            indent=1,
            cls=JSONMessageEncoder
        )
        self.redis_conn.rpush(self.name, resp)

    def get(self, block, timeout):
        raise Queue.Empty


class RecordQueueConsumer:

    def __init__(self, stream_name, redis_conn, pretty_print=True):
        self.stream_name = stream_name
        self.pretty_print = pretty_print
        self.redis_conn = redis_conn
        self.name = get_queue_name(stream_name)

    def state_shard_id(self, shard_id):
        return '_'.join([self.stream_name, shard_id])

    def checkpoint(self, shard_id, seq):
        self.redis_conn.set(shard_id, seq)

    def get(self):
        resp = self.redis_conn.lpop(self.name)
        if resp:
            resp = json.loads(resp)
            shard_id = resp["ShardId"]
            return shard_id, resp
        return None, None

    def start(self):
        while True:
            shard_id, response = self.get()
            if response:
                state_shard_id = self.state_shard_id(shard_id)
                for item in response['Records']:
                    self.process_message(item)
                    self.checkpoint(state_shard_id, item['SequenceNumber'])

    def process_message(self, message):
        def _parse(message):
            message = json.loads(message,
                                 encoding="utf-8")
            self.handle_message(message)
            self.print_message(message)

        try:
            _parse(message['Data'])
        except UnicodeEncodeError as exc:
            if exc.encoding == 'ascii':
                _parse(message['Data'].encode("utf-8"))
            else:
                print("Message Error: %s" % message)
        except:
            print(message['Data'])
            print(traceback.format_exc())

    def print_message(self, message):
        if self.pretty_print:
            indent = 2
            print(json.dumps(message, indent=indent))
        else:
            print(json.dumps(message))

    def handle_message(self, message):
        """
        Override this function in your consumer class to do some stuff with message
        """
        pass
