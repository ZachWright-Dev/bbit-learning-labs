import pika
import os

from consumer_interface import mqConsumerInterface
con_params = pika.URLParameters(os.environ["AMQP_URL"])
connection = pika.BlockingConnection(parameters=con_params)

class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_key, exchange_name, queue_name):
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.q_name = queue_name

        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service

        # Establish Channel
        self.channel = connection.channel()

        # Create Queue if not already present
        self.channel.queue_declare(queue="queue_name")

        # Create the exchange if not already present
        self.channel.exchange_declare(
            exchange="exchange_name", exchange_type="topic"
        )

        # Bind Binding Key to Queue on the exchange
        self.channel.queue_bind(
            queue= "queue_name",
            routing_key= "routing_key",
            exchange="exchange_name",
        )

        # Set-up Callback function for receiving messages
        self.channel.basic_consume("queue_name", self.on_message_callback, auto_ack=False)

    def on_message_callback(self, channel, method_frame, header_frame, body) -> None:
        # Acknowledge message
        self.channel.basic_ack(method_frame.delivery_tag, False)

        #Print message (The message is contained in the body parameter variable)

        print(body)

    def startConsuming(self) -> None:
        # Print " [*] Waiting for messages. To exit press CTRL+C"
        print("[*] Waiting for messages. To exit press CTRL+C")

        # Start consuming messages
        self.channel.start_consuming()
    
    def __del__(self) -> None:
        # Print "Closing RMQ connection on destruction"
        print("Closing RMQ connection on destruction")
        
        # Close Channel
        self.channel.close()

        # Close Connection
        connection.close()
 