#!/bin/bash
##java -classpath lib/*:. com.streamsets.pipeline.sdk.Bootstrap

java -classpath lib/*:. -Djob_id=no47 -Djdbc.url=jdbc:mysql://106.14. -Djdbc.user=root -Djdbc.pwd=*** -Dbinlog.ignoreTables=mysql.%,sys_info.% -Dtopic.no47_crm_customer=%.%customer% -Dtopic.no47_crm_trade=%.trade com.streamsets.pipeline.sdk.Bootstrap
