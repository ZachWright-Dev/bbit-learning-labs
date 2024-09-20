import os
import sys

import pika

from producer_interface import mqProducerInterface

con_params = pika.URLParameters(os.environ["AMQP_URL"])
connection = pika.BlockingConnection(parameters=con_params)

ticker = sys.argv[1]
price = sys.argv[2]
sector = sys.argv[3]

class mqProducer(mqProducerInterface):
    def __init__(self, routing_key, exchange_name ):
        self.routing_key = routing_key
        self.exchange_name = exchange_name

        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        self.channel = connection.channel()
        self.exchange = self.channel.exchange_declare(exchange=self.exchange_name, exchange_type="topic")

    
    def publishOrder(self, message: str) -> None:
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body=f"{ticker} is {price}",
        )
        self.channel.close()
        connection.close()
        
    

        

        