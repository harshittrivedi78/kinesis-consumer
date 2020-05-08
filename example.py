import threading

from kinesis_stream.consumer import KinesisConsumer
from kinesis_stream.record_queue import RecordQueueConsumer
from kinesis_stream.redis_wrapper import get_redis_conn

redis_conn = get_redis_conn(host="localhost", port=6379, db="0")

stream_name = "test-kinesis-stream"
region = "eu-west-1"
redis_state_key = "default-127.0.0.1-6379-0"  # <key-host-port-db>

kinesis_consumer = KinesisConsumer(stream_name, region, redis_conn)
# kinesis_consumer.start()
record_queue_consumer = RecordQueueConsumer(stream_name, redis_conn)
# record_queue_consumer.start()
kinesis_consumer_thread = threading.Thread(name='kinesis_consumer', target=kinesis_consumer.start)
kinesis_consumer_thread.start()

record_queue_consumer_thread = threading.Thread(name='record_queue_consumer', target=record_queue_consumer.start)
record_queue_consumer_thread.start()
