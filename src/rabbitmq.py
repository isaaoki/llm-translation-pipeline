import pika 
import os 
import time
import logging 

class RabbitMQ:
    def __init__(self, exchange):
        self.user = os.getenv("RABBIT_USER")
        self.password = os.getenv("RABBIT_PASS")
        self.host = os.getenv("RABBIT_HOST")
        self.port = int(os.getenv("RABBIT_PORT", "5672"), 10)
        self.vhost = os.getenv("RABBIT_VHOST")
        self.exchange = exchange
        self.connection = None 
        self.channel = None 
        self.logger = logging.getLogger("RABBITMQ")

        self.connect()

    def connect(self, retries=5, delay=5):
        """
        Connects to RabbitMQ server, using credentials
        """
        credentials = pika.PlainCredentials(self.user, self.password)

        for i in range(1, retries + 1):
            try:
                self.logger.info("Attempt %d to connect...", i)
                
                parameters = pika.ConnectionParameters(
                    host=self.host, 
                    port=self.port,
                    virtual_host=self.vhost, 
                    credentials=credentials,
                    heartbeat=1800, # Timeout interval used to detect dead TCP connections
                    blocked_connection_timeout=1800 # Maximum time app will wait while connection is blocked by RabbitMQ
                )

                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()

                self.channel.exchange_declare(
                    exchange=self.exchange,
                    exchange_type="topic",
                    durable=True
                )

                self.logger.info("Connected successfully!")
                return 
            except pika.exceptions.AMQPConnectionError as e:
                self.logger.error("Connection failed: %s", e)

                if i < retries:
                    self.logger.info("Retrying in %ds...", delay)
                    time.sleep(delay)
                else:
                    self.logger.error("Max retries reached. Exiting")
                    raise RuntimeError("Max retries reached. Failed to connect to RabbitMQ")

    def close(self):
        """ 
        Closes connection to RabbitMQ server
        """
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            self.logger.info("Closed connection...")
    
    def consume(self, queue_name, callback):
        """
        Consumes the queue_name, adding a callback function to when receives a message
        """
        if not self.channel:
            self.logger.error("Connection is not established")
            raise RuntimeError("Connection is not established")
        
        # Check if queue exists
        self.channel.queue_declare(queue=queue_name, durable=True)
        # Limit the number of messages to 1
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=queue_name, on_message_callback=callback)
        self.logger.info("Started consuming...")
        self.channel.start_consuming()
    
    def publish(self, routing_key, message):
        """
        Sent message to queue
        """
        if not self.channel:
            self.logger.error("Connection is not established")
            raise RuntimeError("Connection is not established")
        
        self.channel.basic_publish(exchange=self.exchange, routing_key=routing_key, body=message, properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))