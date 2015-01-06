import kombu
import kombu_multibroker
import time

if __name__ == '__main__':
  #host: localhost:5672
  #userid: guest
  #password: guest@localhost:5673,guest:guest
  # conn = kombu.BrokerConnection("multiamqp://guest:guest@localhost:5672/,guest:guest@localhost:5673/")
  #with kombu.BrokerConnection("multiamqp://guest:guest@localhost:5673//,guest:guest@localhost:5672") as conn:
  with kombu.Connection("multiamqp://guest:guest@localhost:5673//") as conn:
    print 'multibroker'
    simple_queue = conn.SimpleQueue('simple_queue_all')
    while True:
      message = simple_queue.get(block=True, timeout=1)
      message.ack()
      print("Received: %s" % message.payload)
      time.sleep(1)
