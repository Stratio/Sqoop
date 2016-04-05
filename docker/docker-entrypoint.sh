#!/bin/bash -xe

 sed -i "s|spark.master.*|spark.master=\${SPARK_MASTER:=local\\[4\\]}|" /etc/sds/sqoop-server/spark-defaults.conf

 /etc/init.d/sqoop-server start
 tail -f /var/log/*