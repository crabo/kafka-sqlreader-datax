#!/bin/bash
heap=384m
##zk_host=10.24.41.55:2181  --master zk://$zk_host/mesos --zk_hosts $zk_host
java -Xmx"$heap" -cp lib/*.jar com.streamsets.pipeline.sdk.Bootstrap