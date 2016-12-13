# kafka-sqlreader-datax
A delta ETL flow trigger by BINLOG in kafka messages.

Connect with  StreamSets DataCollector/ Alibaba DataX
https://github.com/streamsets/datacollector/mysql-binlog-lib
https://github.com/alibaba/datax

get your own package
mvn package -Dmaven.test.skip=true

MySql BinLog origin
This package is build for pulling realtime Ali RDS mysql cluster into out ElasticSearch clusters.

|| Master-Binlog || ==> || Consumer as slave(THIS PROJECT) || ==> || Kafka Bus ||

==> || Kafka Consumer(configurable pipeline in xml,load data in binlog or reload from mysql) to JSON ||

==> || ElasticSearch ||
