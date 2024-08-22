import socket
import logging
import logging_config
import time
from kafka_clients.Producer_ import Producerclass, callback
        
if __name__ == '__main__':   

    # logging configuration
    logging_config.configure_logging()  
   
    # inputs
    bootstrap_servers = "localhost:9092"
    client_id = socket.gethostname()
    topic = 'Test_topic'

    Producer = Producerclass(bootstrap_servers, client_id,topic)
    logging.info(f'ProducerClass object created with name Producer.')

    try:
        timeout_seconds = 30
        start_time = time.time()  # Record the current time
        
        while (time.time() - start_time) < timeout_seconds:
            message = input("Enter any message: ")
            Producer.send_message(message, callback)

    except KeyboardInterrupt:
        pass
    
    Producer.Poll_messages()
    Producer.flush_message()
