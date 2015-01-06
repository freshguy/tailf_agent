"""

Example producer that sends a single message and exits.

You can use `complete_receive.py` to receive the message sent.

"""
from kombu import Connection, Producer, Exchange, Queue

#: By default messages sent to exchanges are persistent (delivery_mode=2),
#: and queues and exchanges are durable.
exchange = Exchange('kombu_demo', type='direct')
queue = Queue('kombu_demo', exchange, routing_key='kombu_demo')


#with Connection('amqp://guest:guest@localhost:5672//') as connection:
with Connection( \
    'amqp://guest:guest@localhost:5672//;amqp://guest:guest@localhost:5673//', \
    failover_strategy='round-robin') as connection:

    #: Producers are used to publish messages.
    #: a default exchange and routing key can also be specifed
    #: as arguments the Producer, but we rather specify this explicitly
    #: at the publish call.
    print '*****************'
    producer = Producer(connection)

    #: Publish the message using the json serializer (which is the default),
    #: and zlib compression.  The kombu consumer will automatically detect
    #: encoding, serialization and compression used and decode accordingly.
    cnt = 0
    while cnt < 1000:
      producer.publish({'hello': 'world'},
                       exchange=exchange,
                       routing_key='kombu_demo',
                       serializer='json', compression='zlib')
      cnt += 1
