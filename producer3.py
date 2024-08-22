# Producer to consistently produces messages into
# a topic with 3 partions so as to enable the use of
# 3 consumers in a single consumer group to read the
# messages from each partition

import socket
import logging_config
import time
from confluent_kafka import Producer
from kafka_clients.Producer_ import Producerclass, callback


if __name__ == '__main__':

    # logging configuration
    logging_config.configure_logging()  
   
    # inputs
    bootstrap_servers = "localhost:9092"
    client_id = socket.gethostname()
    topic = 'Test_topic3'

    Producer = Producerclass(bootstrap_servers, client_id,topic)
    
    try:
        print('Producing...')
        print('Press Ctrl+C to interrupt producing')
        while True:
            Producer.send_message(
                message='Message to Consumer 1', 
                callback= callback, 
                message_key= 'Msg 1', 
                partition_no= 0
                )
            
            Producer.send_message(
                message='Message to Consumer 2', 
                callback= callback, 
                message_key= 'Msg 2', 
                partition_no= 1
                )
            
            Producer.send_message(
                message='Message to Consumer 3', 
                callback= callback, 
                message_key= 'Msg 3', 
                partition_no= 2
                )
            
            time.sleep(5.0) # seconds delay
            Producer.Poll_messages()
    except KeyboardInterrupt:
        pass
    
    finally:
        Producer.Poll_messages()
        Producer.flush_message()