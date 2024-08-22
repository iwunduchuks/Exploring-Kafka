from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaError
import logging

class Adminclass:
    """Admin class for managing Kafka topics."""

    def __init__(self, bootstrap_servers):
        """Initializes the AdminClient."""
        self.bootstrap_servers = bootstrap_servers
        self.config = {'bootstrap.servers': self.bootstrap_servers}
        self.admin = AdminClient(self.config)

    def topic_exists(self, topic_name):
        """Checks if a topic exists.

        Args:
            topic_name (str): Name of the topic to check.

        Returns:
            bool: True if the topic exists, False otherwise.
        """
        all_topics = self.admin.list_topics()
        return topic_name in all_topics.topics.keys()

    def Create_new_topic(self, topic_name, No_of_partitions):
            Topic_list = []
            for i in range(len(topic_name)):
                Topic_list.append(NewTopic(topic_name[i], num_partitions= No_of_partitions[i]))
            
            fs = self.admin.create_topics(Topic_list)
            
            # Call create_topics to asynchronously create topics. A dict
            # of <topic,future> is returned.
            for topic, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    logging.info(f'New topic created: {topic}')
                    print(f'New topic created: {topic}')

                # except Exception as e:
                #     print("Failed to create topic {}: {}".format(topic, e))
       
                except Exception as e:
                    error = e.args[0]

                    if error.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                        logging.exception(f'Topic already exists:{error.str()}')
                        print(f'Kafka Exception: {error.str()}')
                        print('Topic already exists')

    def List_all_topics(self):
        """Reurns all topics in kafka broker.
        """
        all_topics = self.admin.list_topics().topics.keys() # dictionary acessing index (topics) by keys
        return all_topics
