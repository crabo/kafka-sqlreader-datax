# kafka-sqlreader-datax
A delta ETL flow trigger by BINLOG in kafka messages.

# Connect with  StreamSets DataCollector/ Alibaba DataX
https://github.com/streamsets/datacollector/mysql-binlog-lib
https://github.com/alibaba/datax

# get your own package
mvn package -Dmaven.test.skip=true

# MySql BinLog origin
This package is build for pulling realtime Ali RDS mysql cluster into out ElasticSearch clusters.

|| Master-Binlog || ==> || Consumer as slave(THIS PROJECT) || ==> || Kafka Bus ||

==> || Kafka Consumer(configurable pipeline in xml,load data in binlog or reload from mysql) to JSON ||

==> || ElasticSearch ||



1. BinlogPublisher 连接到RDS主机， 实时订阅库、表变更通知， 根据合并规则发布到Kafka多个topic
2. 将BinlogSubscriber发布到dataX的 reader plugin （请先熟悉alibaba DataX）
3. 启动dataX， binlogSubscriber将从Kafka信息触发为对应的sql语句。 查询结果集标准流入到dataX管道， 可以被任何writer消费。
: 只要对binlogSubscriber略加修改， 可以实现binlog直达writer。 适合单表-单表的复制情况。
: 当前实现了更为复杂的“单表变更” --> “通知” --> "SQL查询" --> “DataX Writer” 的流程。
