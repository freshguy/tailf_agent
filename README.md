tailf_agent
===========

A log file 'tailf' producer-agent server for rabbitmq cluster

这是一个类似tailf命令的日志文件检测及按行收集、聚合日志文件的agent服务器。它作为rabbitmq服务器集群的producer端，将收集到的日志按照预设推送到指定的queue中，只要按照示例简单编写相应的rabbitmq集群的consumer端，即可聚合日志内容到任何想要的地方。

特性：
1）可以同时监测多个日志文件，每个检测文件对象可以有各自的配置参数（在monitor_file.cfg文件中）；
2）会在gdbm数据库中记录每个被检测对象各自的更新点（checkPoint），这样服务每次重启都可接着上次的更新点而保证不丢失日志数据；
3）基于日志文件一般是按照日期轮转的特性（如被监测日志文件的日期后缀是20150106），服务器处理了各种按照日期后缀平滑切换具体检测日志文件的情形。如隔了几天后才启动服务器，那么具体的检测日志文件是从上次记录的最后一个日志文件逐个切换到当前日期的（如上次记录的最后一个日志文件是mqlog.20141230,当前日期是20150106，那么会从mqlog.20141230逐步切换到mqlog.20150106）；
4）支持rabbitmq服务器集群。即tailf_agent服务器可以连接到rabbitmq集群上，其本身具有failover、负载均衡功能（当前rabbitmq集群采用镜像配置模式）
