from redis import Redis


class Consumer:
    """
    Consumer for receive from redis stream
    """

    def __init__(self, host: str = "localhost", port: int = 6379):
        """
        Initialize consumer's connection
        """
        self.redis = Redis(host, port, retry_on_timeout=True)

    def get_data(self, stream_key: str = "test"):
        last_id = 0
        sleep_ms = 500

        while True:
            resp = self.redis.xread(
                {stream_key: last_id}, count=1, block=sleep_ms
            )
            if resp:
                key, messages = resp[0]
                last_id, data = messages[0]
                print("REDIS ID: ", last_id)
                print("      --> ", data)


c = Consumer()
c.get_data()