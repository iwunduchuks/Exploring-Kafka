from confluent_kafka import Consumer
from confluent_kafka import TopicPartition
import logging

class ConsumerClass:
    ''' Class to consume messages from kafka 
    '''

    def __init__(self, 
                 bootstrap_server, 
                 topic, 
                 group_id):

        # inputs for consumer configuartion
        self.bootstrap_server = bootstrap_server
        self.group_id = group_id

        self.topic = topic
        
        # configuration
        self.config = {
            "bootstrap.servers": bootstrap_server, 
            "group.id": self.group_id
        }

        # initialising consumer
        self.consumer = Consumer(self.config)

    def subscribe_topic(self):
        # Subscribing to Topic
        self.consumer.subscribe([self.topic])
        logging.info(f"Successfully subscribed to topic: {self.topic}")
        print(f'Successfully subscribed to topic: {self.topic}')

    def assign_parttion(self,partition_no):
        # Assigning a list of partitions
        Partition = TopicPartition(topic= self.topic, partition= partition_no)
        self.consumer.assign([Partition])
        print(f'Assigned to partition: {partition_no}')

    def consume_messages(self):
        try:
            print('Beginning to Consume....')
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logging.error(f"Consumer error: {msg.error()}")
                    print(f'Consumer error: {msg.error()}')
                    continue
                
                message = msg.value().decode("utf-8")

                logging.info(f"message: {message}, Type: {type(message)}")
                print(f'Message: {message}, Key: {type(msg.key().decode('utf-8'))}')
        except KeyboardInterrupt:
            print('Keyboard interrupt. Consumer will now close')
        finally:
            self.consumer.close()