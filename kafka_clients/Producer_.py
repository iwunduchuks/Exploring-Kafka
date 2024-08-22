import logging
from confluent_kafka import Producer

class Producerclass:

    ''' Class to Produce messages to kafka 
    '''

    def __init__(
                 self, 
                 bootstrap_servers, 
                 client_id, 
                 topic
                 ):

        # inputs for producer configuration
        self.bootstrap_server = bootstrap_servers
        self.client_id = client_id

        # inputs to produce message
        self.topic = topic

        # configuration
        self.conf = {
            'bootstrap.servers': self.bootstrap_server,
            'client.id': self.client_id
        }
        
        self.producer = Producer(self.conf)
    
    def send_message(self, message, callback, message_key= None, partition_no=None):
        if partition_no == None and message_key == None:
            self.producer.produce(
                                topic= self.topic, 
                                value= message,  
                                on_delivery= callback
                                )
            
        elif partition_no == None:
            self.producer.produce(
                                topic= self.topic, 
                                value= message,  
                                key= message_key, 
                                on_delivery= callback
                                )
        
        elif message_key == None:
            self.producer.produce(
                                topic= self.topic, 
                                value= message,  
                                partition= partition_no,
                                on_delivery= callback
                                )
            
        else:
            self.producer.produce(
                                topic= self.topic, 
                                value= message,  
                                key= message_key, 
                                partition= partition_no,
                                on_delivery= callback
                                )
        

    def flush_message(self):
        self.producer.flush()

    def Poll_messages(self):
        self.producer.poll(1) # 1 second timeout

def callback(err, msg):
    if err is not None:
        logging.error(f'Failed to Deliver message: {err}.')
    else:
        logging.info(f'Producer success: {msg.value().decode('utf-8')}, Partition: {msg.partition}')