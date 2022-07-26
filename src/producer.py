from redis import Redis
from uuid import uuid4


class Producer:
    """
    Producer for send redis stream
    """

    def __init__(self, host: str = "localhost", port: int = 6379):
        """
        Initialize producer's connection
        """
        self.redis = Redis(host, port, retry_on_timeout=True)

    def send(self, stream_key: str, data: dict):
        try:
            resp = self.redis.xadd(stream_key, data)
            print(resp)
        except ConnectionError as e:
            print("ERROR REDIS CONNECTION:{}".format(e))


def send_message(
        host: str = "localhost",
        port: int = 6379,
        stream_key: str = "test",
        data=None):
    """
    Simplified function to send message with Python / Robot framework
    """
    if data is None:
        data = {"uuid": uuid4().hex}
    p = Producer(host=host, port=port)
    p.send(stream_key=stream_key, data=data)


send_message()
