# kinesis-stream-consumer
Kinesis stream consumer channelize through redis along with aws autorefreshable session 

## Usage

### Requirements

* python >= 3.0
* boto3 >= 1.13.5
* kinesis-python >= 0.2.1
* redis >= 3.5.0

### Installation

Install with:

```shell
pip install kinesis-stream-consumer
```

Or, if you're using a development version cloned from this repository:

```shell
git clone https://github.com/harshittrivedi78/kinesis-stream-consumer.git
python kinesis-stream-consumer/setup.py install
```
This will install boto3 >= 1.13.5 and kinesis-python >= 0.2.1 and redis >= 3.5.0

# How to use it?
There is two consumer which has to be run parallelly one is kinesis consumer and second is records queue consumer
(redis). I have added a example.py file in this code base which can be used to check and test the code.

```python
import threading

from kinesis_stream.consumer import KinesisConsumer
from kinesis_stream.record_queue import RecordQueueConsumer
from kinesis_stream.redis_wrapper import get_redis_conn

redis_conn = get_redis_conn(host="localhost", port=6379, db="0")

stream_name = "test-kinesis-stream"
region = "eu-west-1"

kinesis_consumer = KinesisConsumer(stream_name, region, redis_conn)
record_queue_consumer = RecordQueueConsumer(stream_name, redis_conn)

kinesis_consumer_thread = threading.Thread(name='kinesis_consumer', target=kinesis_consumer.start)
kinesis_consumer_thread.start()

record_queue_consumer_thread = threading.Thread(name='record_queue_consumer', target=record_queue_consumer.start)
record_queue_consumer_thread.start()
```

Override handle_message func to do some stuff with the kinesis messages.

```python
from kinesis_stream.record_queue import RecordQueueConsumer as BaseRecordQueueConsumer

class RecordQueueConsumer(BaseRecordQueueConsumer):
    def handle_message(self, message):
        # your code
        print(message)
```