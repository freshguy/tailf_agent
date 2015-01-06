#!/usr/bin/env python
# coding=utf-8

import time
import kombu

hosts = ['amqp://guest:guest@localhost:5673//',
         'amqp://guest:guest@localhost:5672//']

connection = kombu.Connection(hosts)
connection.ensure_connection()
connection.connect()
channel = connection.channel()
#exchange = kombu.Exchange('some-exchange')
#queue = kombu.Queue(name='some-queue', exchange=exchange)
exchange = kombu.Exchange('kombu_demo', type='direct')
queue = kombu.Queue('kombu_demo', exchange, routing_key='kombu_demo')

cnt = 0
def callback(body, message):
    global cnt
    print('%d: got msg - %s' % (cnt, body))
    message.ack()
    cnt += 1

consumer = kombu.Consumer(channel, queues=queue, callbacks=[callback])
#consumer.consume()
while True:
    try:
        #consumer = kombu.Consumer(channel, queues=queue, callbacks=[callback])
        consumer.consume()
        connection.drain_events()
        time.sleep(1)
    except connection.connection_errors + connection.channel_errors:
        connection.close()
        print("Host down, connecting to the next one.")
        connection.ensure_connection()
        channel = connection.channel()
        consumer = kombu.Consumer(channel, queues=queue, callbacks=[callback])
        consumer.consume()
