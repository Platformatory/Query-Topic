from confluent_kafka import Consumer, KafkaError, TopicPartition
from datetime import datetime, timedelta
from configparser import ConfigParser, NoOptionError
import argparse
from confluent_kafka.admin import AdminClient
import time
import csv


def ist_to_unix_timestamp(ist_time_str):
    ist_time = datetime.strptime(ist_time_str, '%Y-%m-%d %H:%M:%S')
    return int(ist_time.timestamp() * 1000)

def consume_messages(start_time, end_time, string_input, config, topic, csv_file=None):    
    conf = config
    topic_name = topic
    string_data = string_input
    admin_client = AdminClient(conf)
    consumer = Consumer(conf)
    start_timestamp = ist_to_unix_timestamp(start_time)
    end_timestamp = ist_to_unix_timestamp(end_time)
    metadata = admin_client.list_topics(timeout=10)

    if topic_name in metadata.topics:
        topic_metadata = metadata.topics[topic_name]
    else:
        print(f"Topic '{topic_name}' does not exist.")
    
    partitions = [TopicPartition(topic_name, partition_id, start_timestamp) 
              for partition_id in metadata.topics[topic_name].partitions]
    
    offsets = consumer.offsets_for_times(partitions)
    message_count = 0
    total_count = 0
    print(f"Consuming messages from {start_time} IST to {end_time} IST...\n")
    csv_writer = None

    if csv_file:
        csv_file_handle = open(csv_file, mode='w', newline='', encoding='utf-8')
        csv_writer = csv.writer(csv_file_handle)
        csv_writer.writerow(['timestamp', 'message'])

    for partition_offset in offsets:

        if partition_offset.offset != -1:
            consumer.assign([partition_offset])
            assigned_partitions = consumer.assignment()
            #print(f"Assigned partitions: {assigned_partitions}")
            time.sleep(1)            
            consumer.seek(partition_offset)            
            #print(f"Seeking to offset {partition_offset.offset} for partition {partition_offset.partition}...")

            while True:                
                msg = consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():

                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f"Reached end of partition {partition_offset.partition}.")
                        break

                    else:
                        print(f"Error: {msg.error()}")
                        break

                msg_timestamp = msg.timestamp()[1]

                if msg_timestamp > end_timestamp:
                    break
                else:
                    #print(f"Consumed message from partition {partition_offset.partition}: "f"offset {msg.offset()}, timestamp {datetime.fromtimestamp(msg_timestamp / 1000)}")
                    message_data= msg.value().decode('utf-8')
                    total_count += 1

                    if string_data and string_data in message_data:
                        print(message_data)
                        message_count += 1

                        if csv_writer:
                            csv_writer.writerow([datetime.fromtimestamp(msg_timestamp / 1000), message_data])

                    elif string_data is None:
                        print(message_data)
                        message_count+= 1

                        if csv_writer:
                            csv_writer.writerow([datetime.fromtimestamp(msg_timestamp / 1000), message_data])

    if csv_file:
        csv_file_handle.close()

    #print(f"Number of messages processed = {total_count}") 
    print(f"Number of messages meeting input criteria= {message_count}")
    consumer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Consumer Script with Time Range and Query String")
    parser.add_argument('--properties_file', type=str, required=True, help="Path to the client.properties file")
    parser.add_argument('--start_time', type=str, required=True, help="Start time in 'YYYY-MM-DD HH:MM:SS' format")
    parser.add_argument('--end_time', type=str, required=True, help="End time in 'YYYY-MM-DD HH:MM:SS' format")
    parser.add_argument('--string', type=str, help="String to search for in messages (optional)")
    parser.add_argument('--topic', type=str, required=True, help='Name of topic')
    parser.add_argument('--csv', type=str, help="CSV file name to write messages to (optional)")

    args = parser.parse_args()
    config = ConfigParser()
    config.read(args.properties_file)

    try:
        group_id = config.get('DEFAULT', 'group.id') 
    except NoOptionError as e:
        group_id = "foo"

    try:
        auto_offset_reset = config.get('DEFAULT', 'auto.offset.reset')
    except NoOptionError as e:
        auto_offset_reset = 'earliest'
    
    conf = {
        'bootstrap.servers': config.get('DEFAULT', 'bootstrap.servers'),
        'security.protocol': config.get('DEFAULT', 'security.protocol'),
        'sasl.mechanism': config.get('DEFAULT', 'sasl.mechanisms'),
        'sasl.username': config.get('DEFAULT', 'sasl.username'),
        'sasl.password': config.get('DEFAULT', 'sasl.password'),
        'group.id': group_id,
        'auto.offset.reset': auto_offset_reset,
        'log_level':1
    }

    start_time = args.start_time
    end_time = args.end_time
    query_string = args.string
    topic = args.topic
    csv_file = args.csv

    consume_messages(start_time, end_time, query_string, conf, topic, csv_file)