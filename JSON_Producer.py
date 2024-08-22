from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.schema_registry import Schema,SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import SerializationContext, MessageField
import genson
from pprint import pprint
import json
import logging_config
from kafka_clients.Producer_ import Producerclass, callback
import socket
import time

def create_schema(msg):
    ''' 
    Creates json schema from sample message
    
    Args:
        msg (dict): sample message in which schema will be extracted from
    
    Returns:
        schema (dict): returns a schema as a dictionary object.
    '''

    builder = genson.SchemaBuilder()
    builder.add_object(msg)
    schema = builder.to_schema()
    return schema

def write_schema_to_file(schema, schema_filename, indent=2):
    with open(schema_filename, 'w') as schema_file:
        json.dump(schema, schema_file, indent=indent)
    


if __name__ == '__main__':

    # have a dictionary data to be produced
    # get a schema (either on schema registry or local) check if it validates schema
    # if you dont have schema, use dict to create schema
    # serialize dictionary to json using schema
    # produce json message

    # Dict data
    msg = {
        "id": 1,
        "name": "John Doe",
        "email": "john.doe@example.com",
        "age": 30,
    }

    schema = create_schema(msg)
    schema['title'] = 'Testing_schema'
    schema['properties']['id']['minimum'] = 1
    pprint(schema)
    write_schema_to_file(schema, 'schema.json')

    # convert from dict to json string
    schema_str = json.dumps(schema)

    #Schema registry client configuration
    config = {
        'url': 'http://localhost:8081/'
    }

    schema_registry = SchemaRegistryClient(config)

    # # create schema object
    oSchema = Schema(schema_str,'JSON')

    # Send schema to schema registry
    try:
        schemaId = schema_registry.register_schema('json-schema1', oSchema)
        print('Schema registered. Schema ID: {}'.format(schemaId))
    except SchemaRegistryError as e:
        print(f'Error: {e}')

    all_Schema = schema_registry.get_subjects()
    print(all_Schema)

    # get schema from schema registry
    schemaId = 2
    try:
        schema2 = schema_registry.get_schema(schemaId) # returns schema obj
        pprint(schema2)
        print(type(schema2))
    except SchemaRegistryError as e:
        print(f'Error: {e}')

    # JSON Serialiser
    msg = {
        "id": 1,
        "name": "John Doe",
        "email": "john.doe@example.com",
        "age": 30,
    }

    JsonSer = JSONSerializer(schema2,schema_registry) # serialiser instance
    ctx = SerializationContext('Test_topic1', MessageField.VALUE) # Serialization contect
    json_byte_message = JsonSer.__call__(msg,ctx) # Serializing message to bytes
    print('Message serialized')

    #---------------------------------------------------
    # Producing Json Bytes message
    # logging configuration
    logging_config.configure_logging()  
   
    # inputs
    bootstrap_servers = "localhost:9092"
    client_id = socket.gethostname()
    topic = 'Test_topic1'

    Producer = Producerclass(bootstrap_servers, client_id,topic)
    Producer.send_message(json_byte_message, callback)
    Producer.Poll_messages
    Producer.flush_message
    # time.sleep(5)
    print('Serialized message produced')
    
    


    