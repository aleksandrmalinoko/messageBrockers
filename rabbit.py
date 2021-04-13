from time import sleep, time
from threading import Thread, Event
from pika import BlockingConnection, URLParameters
from queue import Empty, Queue
import config
from datetime import datetime

task_queue = Queue()
stop_request = Event()


def callback(ch, method, properties, body):
    print(f" [x] Received {body}")


class Consumer(Thread):
    def __init__(self):
        super(Consumer, self).__init__()
        self.creds: str = config.creds
        self.url: str = config.url
        self.admin_port: int = config.admin_port
        self.api_port: int = config.api_port
        self.host: str = config.host
        self.queue: str = config.queue
        self.connection = BlockingConnection(URLParameters(self.host))
        self.channel = self.connection.channel()

    def run(self) -> None:
        while not stop_request.isSet():
            try:
                print(' [*] Waiting for messages. To exit press CTRL+C')
                while not task_queue.empty():
                    value = task_queue.get()
                    self.channel.basic_publish(exchange='AlexTest', routing_key=self.queue, body=value)
                    now = datetime.now()
                    print(f"consumed message at {now}")
            except Empty:
                continue
            # finally:
            #     if not self.connection.is_closed:
            #         self.connection.close()
            sleep(3)


class Producer(Thread):
    def __init__(self, node):
        super(Producer, self).__init__()
        self.creds: str = config.creds
        self.url: str = config.url
        self.admin_port: int = config.admin_port
        self.api_port: int = config.api_port
        self.host: str = config.host
        self.queue: str = config.queue
        self.connection = BlockingConnection(URLParameters(self.host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue)
        self.node = node

    @staticmethod
    def extract(data):
        found = []
        for i in data:
            found.append(i)
        return found

    def run(self) -> None:
        while not stop_request.isSet():
            now = datetime.now()
            print(f"published message at {now}")
            data = bytes(f"Hello World!{time}")
            values = self.extract(data)
            for value in values:
                # element = f"produced value {value} at {now}, queue size is {task_queue.qsize() + 1}"
                # print(element)
                print("Your message:", value)
                task_queue.put(value)
            sleep(5)


if __name__ == '__main__':
    Producer("TEST").start()
    Consumer().start()
