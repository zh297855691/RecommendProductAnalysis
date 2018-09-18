#!/bin/bash
/usr/hdp/current/spark-client/bin/spark-submit \
  --master yarn-cluster \
  --class com.cmit.cmhk.GreetingMessageAnalysis \
  --driver-memory "1G" \
  --driver-cores "1" \
  --executor-memory "1G" \
  --num-executors "3" \
  --executor-cores "1" \
  --conf "spark.streaming.kafka.maxRatePerPartition=1000" \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.kryoserializer.buffer=1024k" \
  --conf "spark.shuffle.blockTransferService=netty" \
  --conf "spark.io.compression.codec=snappy" \
  --conf "spark.io.compression.snappy.blockSize=1024k" \
  --conf "spark.shuffle.sort.bypassMergeThreshold=2000" \
  --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./jaas-cache.conf" \
  --driver-java-options "-Djava.security.auth.login.config=./jaas-cache.conf" \
  --queue mrs \
  --files $(dirname "$PWD")/conf/cmhk-config.properties,$(dirname "$PWD")/conf/cmhk-log.properties,$(dirname "$PWD")/conf/jaas-cache.conf,$(dirname "$PWD")/conf/mrs.keytab,$(dirname "$PWD")/conf/krb5.conf \
  --jars $(dirname "$PWD")/lib/c3p0-0.9.1.2.jar,$(dirname "$PWD")/lib/mysql-connector-java-5.1.39.jar,$(dirname "$PWD")/lib/spark-examples-1.6.2.2.5.3.0-37-hadoop2.7.3.2.5.3.0-37.jar,/usr/hdp/2.5.3.0-37/hbase/lib/hbase-common-1.1.2.2.5.3.0-37.jar,/usr/hdp/2.5.3.0-37/hbase/lib/hbase-protocol-1.1.2.2.5.3.0-37.jar,/usr/hdp/2.5.3.0-37/hbase/lib/metrics-core-2.2.0.jar  \
  ./RecommendProductAnalysis.jar
  
  
  
  
  
  
  
