#!/usr/bin/env python
# coding=utf-8

import os
import sys
import time
import kombu

def on_return_callback(exception, exchange, routing_key, message):
  print 'An undeliverable message. exception: %s, routing_key: %s, message: %s' \
      % (exception, routing_key, message)

class HaProducer(object):
  def __init__(self, hosts, \
      queue, routing_key, exchange_type='direct', \
      serializer='json', compression='zlib'):
    self.hosts = hosts
    self.queue = queue
    self.routing_key = routing_key
    self.exchange_type = exchange_type
    self.serializer = serializer
    self.compression = compression
    self.connection = kombu.Connection(hosts)
    self.connection.ensure_connection()
    self.connection.connect()
    self.channel = self.connection.channel()

    self.exchange = kombu.Exchange(self.queue, type=exchange_type)
    #self.queue = kombu.Queue(self.queue, self.exchange, \
    #    routing_key=self.routing_key)

    self.producer = kombu.Producer(self.channel, \
        self.exchange, routing_key=self.routing_key, \
        serializer=self.serializer, \
        compression=self.compression, \
        on_return=on_return_callback)

  def producer_single(self, msg_body_str):
    try:
      self.producer.publish(msg_body_str, \
          exchange=self.exchange, \
          routing_key=self.routing_key, \
          serializer=self.serializer, \
          compression=self.compression)
      #print 'before drain_events'
      #self.connection.drain_events()
      #print 'after drain_events'
    except self.connection.connection_errors + self.connection.channel_errors:
      self.connection.close()
      print("[%s:%s] host breakdown, switch to next one." % \
          (self.connection.hostname, self.connection.port))
      self.connection.ensure_connection()
      self.channel = self.connection.channel()
      self.producer = kombu.Producer(self.channel, \
          self.exchange, routing_key=self.routing_key, \
          serializer=self.serializer, \
          compression=self.compression, \
          on_return=on_return_callback)

      self.producer.publish(msg_body_str, \
          exchange=self.exchange, \
          routing_key=self.routing_key, \
          serializer=self.serializer, \
          compression=self.compression)


if __name__ == '__main__':
  hosts = ['amqp://guest:guest@localhost:5673//',
           'amqp://guest:guest@localhost:5672//']
  queue = 'kombu_demo'
  exchange_type = 'direct'
  routing_key = 'kombu_demo'
  serializer = 'json'
  compression = 'zlib'

  ha_producer = HaProducer(hosts, queue, routing_key, \
      exchange_type, serializer, compression)
  msg_body_str = "{\'hello\':\'world\'}"
  print '------ single producer ------'
  ha_producer.producer_single(msg_body_str);

  print '------ batch producer ------'
  cnt = 0
  while cnt < 1000:
    try:
      ha_producer.producer_single(msg_body_str)
      print 'ha_producer cnt: %s' % cnt
    except Exception as e:
      print 'batch produce exception: %s' % e
    cnt += 1
    time.sleep(1)
