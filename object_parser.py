#!/usr/bin/env python
# coding=utf-8

import os
import time
import json
#from datetime import *

def next_day_str(cur_day_str, timeformat='%Y%m%d'):
  cur_day_timestamp = \
    time.mktime(time.strptime(cur_day_str, timeformat))
  next_day = time.localtime(cur_day_timestamp + 3600 * 24)
  return time.strftime(timeformat, next_day)

def pre_day_str(cur_day_str, timeformat='%Y%m%d'):
  cur_day_timestamp = \
    time.mktime(time.strptime(cur_day_str, timeformat))
  pre_day = time.localtime(cur_day_timestamp - 3600 * 24)
  return time.strftime(timeformat, pre_day)

class MonitorObject(object):
  def __init__(self, object_dict):
    self.file_path = object_dict['file_path']
    self.file_pattern = object_dict['file_pattern']
    self.queue = object_dict['queue']
    self.exchange_type = object_dict['exchange_type']
    self.routing_key = object_dict['routing_key']
    join_str = '/' if object_dict['file_path'][-1] != '/' else ''
    self.checkpoint_name = object_dict['file_path'] + join_str + object_dict['file_pattern']

    '''
    for k, v in object_dict.items():
      setattr(self, k, v)
    self.__dict__
    '''

  def object_print(self, ):
    for k, v in self.__dict__.iteritems():
        if k in ['file_path', 'file_pattern', 'checkpoint_name', 'queue']:
            print '%s : %s' % (k, v)

class ProjectObject(object):
  def __init__(self, project_name):
    self.project_name = project_name
    self.monitor_objects = []

  def objects_add(self, monitor_object):
    self.monitor_objects.append(monitor_object)

  def project_print(self, ):
    print 'project_name: %s\n' % (self.project_name, )
    for item in self.monitor_objects:
      print '------------------------------------------------------------'
      item.object_print()

class ObjectParser:
  def __init__(self, config_file):
    self.object_config_file = config_file
    self.checkpoint_file = ''
    self.project_objects = []

  def parse(self, ):
    config_f = file(self.object_config_file)
    json_value = json.load(config_f)
    config_f.close
    self.checkpoint_file = json_value['checkPointFile']
    # print 'checkpoint_file: %s' % self.checkpoint_file
    projects = json_value['configs']['projects']
    for project in projects:
      project_name = project['projectName']
      project_object = ProjectObject(project_name)
      files = project['files']
      for monitor_file in files:
        object_dict = dict((
          ('file_path', monitor_file['filePath']),
          ('file_pattern', monitor_file['filePattern']),
          ('queue', monitor_file['queue']),
          ('exchange_type', monitor_file['exchange_type']),
          ('routing_key', monitor_file['routing_key']),
          ))
        monitor_object = MonitorObject(object_dict)
        project_object.objects_add(monitor_object)
      self.project_objects.append(project_object)

  def parser_print(self, ):
    print 'object_config_file: %s' % self.object_config_file
    for project_object in self.project_objects:
      print '********************** ProjectObject ************************'
      project_object.project_print()

if __name__ == "__main__":
  object_dict = dict((
    ('file_path', '/home/wangjuntao/devspace/logs/'),
    ('file_pattern', 'biz_'),
    ('queue', 'uzhu_log'),
    ('exchange_type', 'direct'),
    ('routing_key', 'abc'),
    ))
  monitor_object0 = MonitorObject(object_dict)
  monitor_object1 = MonitorObject(object_dict)

  project_object = ProjectObject('ustore')
  project_object.objects_add(monitor_object0)
  project_object.objects_add(monitor_object1)

  project_object.project_print()

  object_parser = ObjectParser(r'./monitor_file.cfg')
  object_parser.parse()
  object_parser.parser_print()

  print 'strptime: %s' % time.strptime('20141212', '%Y%m%d')
  print '--------------------------'
  timestamp = time.mktime(time.strptime('20141231', '%Y%m%d'))
  print 'mktime: %s' % timestamp
  timestamp = timestamp + 3600 * 24
  nextday = time.localtime(timestamp)
  print 'strftime(next day): %s' % time.strftime('%Y%m%d', nextday)
  #date_time = datetime.strptime('20141231', '%Y%m%d')

  cur_day_str = '20141214'
  cnt = 0
  while cnt < 370:
    cur_day_str = next_day_str(cur_day_str)
    print '%s: %s' % (cnt, cur_day_str)
    cnt = cnt + 1

  while cnt >= 0:
    cur_day_str = pre_day_str(cur_day_str)
    print '%s: %s' % (cnt, cur_day_str)
    cnt = cnt - 1
