#!/usr/bin/env python
# coding=utf-8

import os
import sys
import time
import kombu

consume_cnt = 0

def callback_single(body, message):
  global consume_cnt
  print ('%d: (single)got msg - %s' % (consume_cnt, body))
  message.ack()
  consume_cnt += 1

class HaConsumer(object):
  def __init__(self, hosts, \
      queue, routing_key, exchange_type='direct'):
    self.hosts = hosts
    self.queue = queue
    self.exchange_type = exchange_type
    self.routing_key = routing_key
    self.connection = kombu.Connection(hosts)
    self.connection.ensure_connection()
    self.connection.connect()
    self.channel = self.connection.channel()
    print 'queue: %s' % self.queue
    print 'type: %s' % self.exchange_type
    print 'routing_key: %s' % self.routing_key
    self.exchange = kombu.Exchange(self.queue, type=exchange_type)
    self.queue = kombu.Queue(self.queue, self.exchange, \
        routing_key=self.routing_key)

    self.consumer = kombu.Consumer(self.channel, \
        queues=self.queue, callbacks=[callback_single])

  '''
  @staticmethod
  def callback_single(body, message):
    print ('%d: (single)got msg - %s' % (self.consume_cnt, body))
    message.ack()
    self.consume_cnt += 1
  '''  


  def consumer_single (self, ):
    try:
      self.consumer.consume()
      self.connection.drain_events()
    except self.connection.connection_errors + self.connection.channel_errors:
      self.connection.close()
      print("[%s:%s] host breakdown, switch to next one." % \
          (self.connection.hostname, self.connection.port))
      self.connection.ensure_connection()
      self.channel = self.connection.channel()
      self.consumer = kombu.Consumer(self.channel, \
          queues=self.queue, callbacks=[callback_single])
      self.consumer.consume()


if __name__ == '__main__':
  hosts = ['amqp://guest:guest@localhost:5673//',
           'amqp://guest:guest@localhost:5672//']
  #queue = 'kombu_demo'
  #exchange_type = 'direct'
  #routing_key = 'kombu_demo'
  queue = 'biz_queue'
  exchange_type = 'direct'
  routing_key = 'ustore'

  ha_consumer = HaConsumer(hosts, queue, routing_key, exchange_type)
  print '------ singel consume ------'
  ha_consumer.consumer_single()

  print '------ batch consume ------'
  while True:
    try:
      ha_consumer.consumer_single()
    except Exception as e:
      ##   Queue.declare: (404) NOT_FOUND - home node 'rabbit@dev' of durable queue 'kombu_demo' in vhost '/' is down or inaccessible
      ## 在停止了rabbit节点(rabbit是建立镜像节点时候的主节点)的时候抛以上的异常，是因为produce的时候(只在rabbit中声明了名为kombu_demo的queue)，
      ## 而并没有真正的将kombu_demo声明到所有的镜像节点中去
      print 'batch consume exception: %s' % e
    time.sleep(1)
