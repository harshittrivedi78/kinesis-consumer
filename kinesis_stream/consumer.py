from kinesis.consumer import KinesisConsumer as BaseKinesisConsumer
from kinesis_stream.aws_session import get_autorefresh_session
from kinesis_stream.state import State
from kinesis_stream.record_queue import RecordQueue


class KinesisConsumer:

    def __init__(self, stream_name, region, redis_conn):
        self.stream_name = stream_name
        self.boto3_session = get_autorefresh_session(region)
        self._consumer = BaseKinesisConsumer(self.stream_name, self.boto3_session)
        self._consumer.LOCK_DURATION = 300
        self._consumer.record_queue = RecordQueue(stream_name)
        self._consumer.record_queue.redis_conn = redis_conn
        self._consumer.state = State()
        self._consumer.state.redis_conn = redis_conn

    def start(self):
        for message in self._consumer:
            pass
