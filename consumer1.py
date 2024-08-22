import logging_config
from kafka_clients.Consumer_ import ConsumerClass

if __name__ == "__main__":
    
    # logging configuration
    logging_config.configure_logging()

    bootstrap_server = "localhost:9092"
    topic = 'Test_topic3'
    group_id = 'consumer_group_1'

    consumer1 = ConsumerClass(bootstrap_server, topic, group_id)

    # consumer1.subscribe_topic()
    # Uncomment below and comment subscribe above to use assign() instead of subscribe()
    partition = 0 # partiion must be integer 
    consumer1.assign_parttion(partition)
    
    consumer1.consume_messages()