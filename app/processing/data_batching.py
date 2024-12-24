import toolz as t
from app.kafka_settings.producer import produce

def batch_data(data, batch_size):
    return list(t.partition_all(batch_size, data))

def send_data_to_kafka(data, topic, key, batch_size=1000):
    batches = batch_data(data, batch_size)
    for batch in batches:
        produce(batch, key, topic)