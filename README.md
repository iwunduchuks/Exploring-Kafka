# How to run

1. Make sure Docker Desktop is running in the background

2. In the working directory, run the following code in the terminal. This runs the docker-compose.yml file (would first pull Zookeeper, Broker, Control center and schema registry images)

    ``` bash
    docker compose up -d
    ```

3. Create a topic (Test_topic) from the Control centre UI. You can access the UI through localhost:9021

4. Run producer.py from the terminal and input Messages to be produced to the Kafka broker. You can write as many messages during the period of the timer. Once the timer is elapsed, the script will end once you send a final message.

    ``` bash
    python producer.py
    ```

5. Messages should be found in the 'Test_topic'

Modify code as you please
