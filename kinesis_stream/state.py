class State:
    redis_conn = None

    def __init__(self):
        self.shards = {}

    def get_iterator_args(self, shard_id):
        sequence = self.get_sequence_number(shard_id)
        if sequence:
            args = dict(
                ShardIteratorType='AFTER_SEQUENCE_NUMBER',
                StartingSequenceNumber=sequence.decode()
            )
            return args
        else:
            return dict(
                ShardIteratorType='LATEST'
            )

    def checkpoint(self, shard_id, seq):
        self.shards[shard_id] = seq
        self.redis_conn.set(shard_id, seq)

    def get_sequence_number(self, shard_id):
        return self.redis_conn.get(shard_id)
