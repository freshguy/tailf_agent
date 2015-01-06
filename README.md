tailf_agent
===========

A log file 'tailf' producer-agent server for rabbitmq cluster

这是一个类似tailf命令的日志文件检测及按行收集、聚合日志文件的agent服务器。它作为rabbitmq服务器集群的producer，将收集的日志按照预设推送到制定的queue中，只要编写相应的rabbitmq集群的consumer端，即可聚合日志内容到任何想要的地方。
