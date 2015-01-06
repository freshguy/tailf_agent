#!/usr/bin/env python
# coding=utf-8

import time
from kombu import Connection
from kombu.pools import connections

if __name__ == '__main__':
  #Connection('amqp://foo;amqp://bar')
  '''
  c1 = Connection('amqp://guest:guest@localhost:5672//')
  c2 = Connection('amqp://guest:guest@localhost:5673//')
  with connections[c1].acquire(block=True) as conn1:
      with connections[c2].acquire(block=True) as conn2:
        simple_queue = conn1.SimpleQueue('simple_queue')
        message = simple_queue.get(block=True, timeout=1)
        print("Received: %s" % message.payload)
        message.ack()
        simple_queue.close()
  '''
  #conn = Connection('amqp://guest:guest@localhost:5673//;amqp://guest:guest@localhost:5672//', \
  #    failover_strategy='round-robin')  # round-robin, shuffle
  with kombu.Connection('amqp://guest:guest@localhost:5673//') as conn:
    simple_queue = conn.SimpleQueue('simple_queue_all')
    while True:
      message = simple_queue.get(block=True, timeout=1)
      print("Received: %s" % message.payload)
      message.ack()
      time.sleep(1)

  #c1 = Connection('amqp://guest:guest@localhost:5673//')
  #with connections[c1].acquire(block=True) as conn1:
  #with Connection('amqp://guest:guest@localhost:5672//') as conn:
  '''
    while True:
      #simple_queue = None
      try:
        conn.connect()
      except Exception as e:
        print 'Exception: connect error'
        conn.release()
        continue

      simple_queue = conn.SimpleQueue('simple_queue_all')
      message = simple_queue.get(block=True, timeout=1)
      
      #except Exception as e:
      #  print 'exception: %s' % e
      #  #simple_queue.close()
      #  #conn.release()
      #  continue
      
      print("Received: %s" % message.payload)
      message.ack()
      simple_queue.close()
      conn.release()
      #time.sleep(2)
  '''
