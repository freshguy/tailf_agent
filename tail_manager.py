#!/usr/bin/env python
# coding=utf-8

import os
import re
import sys
import logging
import time
import signal
import threading
import json
# 首先应该执行：sudo apt-get install python-gdbm (libgdbm-dev)
import gdbm
from datetime import datetime, date
import pyinotify
from Queue import Queue
from object_parser import ObjectParser
from ha_producer import HaProducer


class FileMonitorThread(threading.Thread):
  def __init__(self, monitor_object, checkpoint_db, project_name, \
      mq_broker_urls):
    threading.Thread.__init__(self,
      name = 'monitor_thread_name: %s' % monitor_object.checkpoint_name)
    self.monitor_object = monitor_object
    self.checkpoint_db = checkpoint_db
    self.project_name = project_name
    self.mq_broker_urls = mq_broker_urls

  def run(self, ):
    # construct a HaProducer
    self.ha_producer = HaProducer(self.mq_broker_urls,
        self.monitor_object.queue, \
        self.monitor_object.routing_key, \
        self.monitor_object.exchange_type, \
        serializer='raw', \
        compression='zlib')
    
    '''
    cnt = 0
    #msg_body_str = "{\'hello\':\'girl\'}"
    msg_body_str = 'Welcome to Beijing'
    while cnt < 100:
      print 'project_name: %s, cnt: %s' % (self.project_name, cnt)
      self.ha_producer.producer_single(msg_body_str)
      cnt += 1
    '''

    self.check_point_gdbm \
      = gdbm.open(self.checkpoint_db, 'cs')

    checkpoint_name = self.monitor_object.checkpoint_name.encode("utf-8")
    #print 'checkpoint_name: %s' % checkpoint_name

    checkpoint_str = ''
    if checkpoint_name in self.check_point_gdbm:
      checkpoint_str = self.check_point_gdbm[checkpoint_name]
    #print 'checkpoint_str: %s' % checkpoint_str

    if checkpoint_str == '':  # an new monitor_object file
      print 'handle_new_monitor: %s' % checkpoint_name
      self.handle_new_monitor(checkpoint_name)
    else:
      print 'handle_existing_monitor: %s' % checkpoint_name
      self.handle_existing_monitor(checkpoint_name, checkpoint_str)


  def handle_new_monitor(self, checkpoint_name):
    monitored_file_prefix = self.monitor_object.file_path \
      + ('/' if self.monitor_object.file_path[-1] != '/' else '') \
      + self.monitor_object.file_pattern

    monitored_file_suffix \
      = time.strftime("%Y%m%d", time.localtime())

    monitored_file = monitored_file_prefix + monitored_file_suffix
    pre_monitored_file = ''

    old_size = 0
    f = open(monitored_file, 'r')
    file_switch_try_cnt = 0
    while True:
      size = os.path.getsize(monitored_file)
      if size > old_size:
        f.seek(old_size)
        read_line_cnt = 0
        # 每次限制1000行
        while read_line_cnt < 1000:
          size = int(f.tell())
          line = f.readline()
          if not line.endswith('\n'):
            break
          else:
            print '[%s] %s' % (self.project_name, line, )
            try:
              self.ha_producer.producer_single(line)
            except Exception as e:
              print 'HaProducer.producer_single exception: %s' % (e, )
          read_line_cnt += 1

      elif size == old_size:
        current_day_str = time.strftime("%Y%m%d", time.localtime())
        if current_day_str != monitored_file_suffix: # a new day
          file_switch_try_cnt += 1
          # 延迟检测60s
          if file_switch_try_cnt > 60:
            file_switch_try_cnt = 0
            this_monitored_file = monitored_file_prefix + current_day_str
            if os.path.exists(this_monitored_file) == True:
              f.close()  # close 'pre_monitored_file'
              monitored_file_suffix = current_day_str
              pre_monitored_file = monitored_file  # set 'pre_monitored_file'
              monitored_file = this_monitored_file
              f = open(monitored_file, 'r')
              # old_size = 0
              size = 0
            else:
              time.sleep(10)
      else:
        pass

      old_size = size
      ### update checkpoint in gdbm
      checkpoint_str = '%s:%s' % (int(time.time() * 100), old_size)
      self.check_point_gdbm[checkpoint_name] = checkpoint_str
      time.sleep(1)

  def handle_existing_monitor(self, checkpoint_name, checkpoint_str):
    monitored_file_prefix = self.monitor_object.file_path \
      + ('/' if self.monitor_object.file_path[-1] != '/' else '') \
      + self.monitor_object.file_pattern

    split_segs    = checkpoint_str.split(':')
    old_timestamp = int(split_segs[0])
    old_size      = int(split_segs[1])

    print 'old_timestamp: %s' % old_timestamp
    print 'old_size: %s' % old_size

    monitored_file_suffix \
      = datetime.fromtimestamp(old_timestamp / 100)
 
    monitored_file = monitored_file_prefix + monitored_file_suffix
    pre_monitored_file = ''

    print 'monitored_file: %s' % monitored_file
 
    os.exit()
    f = open(monitored_file, 'r')
    file_switch_try_cnt = 0
    while True:
      size = os.path.getsize(monitored_file)
      if size > old_size:
        f.seek(old_size)
        read_line_cnt = 0
        # 每次限制1000行
        while read_line_cnt < 1000:
          size = int(f.tell())
          line = f.readline()
          if not line.endswith('\n'):
            break
          else:
            print '[%s] %s' % (self.project_name, line, )
          read_line_cnt += 1

      elif size == old_size:
        today_day_str = time.strftime("%Y%m%d", time.localtime())
        yestoday_day_str = pre_day_str(today_day_str)

        current_day_str = next_day_str(monitored_file_suffix)
        if current_day_str > monitored_file_suffix && current_day_str < today_day_str:
          file_switch_try_cnt += 1
          if file_switch_try_cnt > 60:
            file_switch_try_cnt = 0
            this_monitored_file = monitored_file_prefix + current_day_str
            if os.path.exists(this_monitored_file) == True:
              f.close()  # close 'pre_monitored_file'
              monitored_file_suffix = current_day_str
              pre_monitored_file = monitored_file  # set 'pre_monitored_file'
              monitored_file = this_monitored_file
              f = open(monitored_file, 'r')
              # old_size = 0
              size = 0
            else:
              time.sleep(10)
        elif current_day_str > monitored_file_suffix && current_day_str == today_day_str:
      else:
        pass

      old_size = size
      ### update checkpoint in gdbm
      checkpointStr = '%s:%s' % (int(time.time() * 100), old_size)
      self.check_point_gdbm[checkpoint_name] = checkpointStr
      time.sleep(1)

class MqNode(object):
  def __init__(self, node_dict):
    self.host       = node_dict['host']
    self.port       = node_dict['port']
    self.user       = node_dict['user']
    self.pwd        = node_dict['pwd']
    self.vhost      = node_dict['vhost']
    self.broker_url = 'amqp://' + self.user + ':' \
        + self.pwd + '@' + self.host + ':' \
        + str(self.port) + '//'
    print 'broker_url: %s' % self.broker_url

  def mq_node_print(self, ):
    for k, v in self.__dict__.iteritems():
      print '%s: %s' % (k, v)

class TailfAgent(object):
  def __init__(self, server_conf_file):
    server_conf_f = file(server_conf_file)
    server_conf_json = json.load(server_conf_f)
    server_conf_f.close

    self.monitor_obj_conf_file = server_conf_json['monitor_obj_conf_file']
    self.object_parser = ObjectParser(self.monitor_obj_conf_file)
    self.object_parser.parse()
    #self.object_parser.parser_print()

    self.checkpoint_file = self.object_parser.checkpoint_file
    self.relay_queue_num \
      = server_conf_json['server_config']['relay_queue_num']
    self.emit_thread_num \
      = server_conf_json['server_config']['emit_thread_num']
   
    self.mq_nodes       = []
    self.mq_broker_urls = []
    mq_nodes = server_conf_json['server_config']['mq_nodes']
    for mq_node in mq_nodes:
      node_dict = dict((
        ('host',  mq_node['host']),
        ('port',  mq_node['port']),
        ('user',  mq_node['user']),
        ('pwd',   mq_node['pwd']),
        ('vhost', mq_node['vhost']),
      ))
      mqNode = MqNode(node_dict)
      self.mq_nodes.append(mqNode)
      self.mq_broker_urls.append(mqNode.broker_url)
    print 'mq_node num: %d' % len(self.mq_nodes)

    for node in self.mq_nodes:
      node.mq_node_print()

  def tail_agent_print(self, ):
    for k, v in self.__dict__.iteritems():
      if k not in ['mq_nodes']:
        print '%s : %s' % (k, v)

  def start_agent_server(self, ):
    total_monitor_obj_cnt = 0
    for project in self.object_parser.project_objects:
      total_monitor_obj_cnt += len(project.monitor_objects)
    print 'total monitor_object cnt: %d' % \
      total_monitor_obj_cnt

    threads = []
    for project in self.object_parser.project_objects:
      project_name = project.project_name
      for monitor_obj in project.monitor_objects:
        threads.append(FileMonitorThread(
          monitor_obj,
          self.checkpoint_file,
          project_name,
          self.mq_broker_urls))

    for t in threads:
      t.start()

if __name__ == '__main__':
  #signal.signal(signal.SIGINT, signal.SIG_DFL)
  #signal.signal(signal.SIGTERM, signal.SIG_DFL)

  if len(sys.argv) <= 1:
    print '[Command]: python tail_manager.py ./server.conf'
    sys.exit()
  server_conf = sys.argv[1]

  tailf_agent = TailfAgent(server_conf)
  tailf_agent.tail_agent_print()
  tailf_agent.start_agent_server()

  '''
  print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
  print time.time()
  print int(time.time() * 100)
  print 'fromtimestamp: %s' % datetime.fromtimestamp(int(time.time() * 100) / 100)
  # print 'fromtimestamp: %s' % datetime.fromtimestamp(141871528476 / 100)
  print date.today()
  print datetime.now().microsecond
  print time.strftime("%Y%m%d", time.localtime())
  '''

  while True:
    time.sleep(10)
