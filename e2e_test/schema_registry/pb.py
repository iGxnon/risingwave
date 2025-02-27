from protobuf import user_pb2
from google.protobuf.source_context_pb2 import SourceContext
import sys
from confluent_kafka import Producer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.value(), err))


def get_user(i):
    return user_pb2.User(
        id=i,
        name="User_{}".format(i),
        address="Address_{}".format(i),
        city="City_{}".format(i),
        gender=user_pb2.MALE if i % 2 == 0 else user_pb2.FEMALE,
        sc=SourceContext(file_name="source/context_{:03}.proto".format(i)),
    )

def get_user_with_more_fields(i):
    return user_pb2.UserWithMoreFields(
        id=i,
        name="User_{}".format(i),
        address="Address_{}".format(i),
        city="City_{}".format(i),
        gender=user_pb2.MALE if i % 2 == 0 else user_pb2.FEMALE,
        sc=SourceContext(file_name="source/context_{:03}.proto".format(i)),
        age=i,
    )

def get_user_with_new_type(i):
    return user_pb2.UserWithNewType(
        id=i,
        name="User_{}".format(i),
        address="Address_{}".format(i),
        city=i,
        gender=user_pb2.MALE if i % 2 == 0 else user_pb2.FEMALE,
        sc=SourceContext(file_name="source/context_{:03}.proto".format(i)),
    )

def send_to_kafka(producer_conf, schema_registry_conf, topic, num_records, get_user_fn, pb_message):
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    serializer = ProtobufSerializer(
        pb_message,
        schema_registry_client,
        {"use.deprecated.format": False, 'skip.known.types': True},
    )

    producer = Producer(producer_conf)
    for i in range(num_records):
        user = get_user_fn(i)

        producer.produce(
            topic=topic,
            partition=0,
            value=serializer(user, SerializationContext(topic, MessageField.VALUE)),
            on_delivery=delivery_report,
        )
    producer.flush()
    print("Send {} records to kafka\n".format(num_records))


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("pb.py <brokerlist> <schema-registry-url> <topic> <num-records> <pb_message>")
        exit(1)

    broker_list = sys.argv[1]
    schema_registry_url = sys.argv[2]
    topic = sys.argv[3]
    num_records = int(sys.argv[4])
    pb_message = sys.argv[5]

    all_pb_messages = {
        'user': (get_user, user_pb2.User),
        'user_with_more_fields': (get_user_with_more_fields, user_pb2.UserWithMoreFields),
        'user_with_new_type': (get_user_with_new_type, user_pb2.UserWithNewType),
    }

    assert pb_message in all_pb_messages, f'pb_message must be one of {list(all_pb_messages.keys())}'

    schema_registry_conf = {"url": schema_registry_url}
    producer_conf = {"bootstrap.servers": broker_list}

    try:
        send_to_kafka(producer_conf, schema_registry_conf, topic, num_records, *all_pb_messages[pb_message])
    except Exception as e:
        print("Send Protobuf data to schema registry and kafka failed {}", e)
        exit(1)
