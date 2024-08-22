import logging_config
from kafka_clients.Admin_ import Adminclass

if __name__ == '__main__':
    
    logging_config.configure_logging()
    bootstrap_server = "localhost:9092"
    Admin = Adminclass(bootstrap_server)

    # Creating a topic
    Topic = ['Test_topic3'] # must be list of strings
    Partition = [3] # must be list of integers
    Admin.Create_new_topic(Topic, Partition)

    # List all topics
    # all_topics = Admin.List_all_topics()
    # print(List_all_topics)
