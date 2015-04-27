#!/bin/bash

java -Xms512M -Xmx1024M -Xss1024K -XX:PermSize=256m -XX:MaxPermSize=512m \
        -cp KafkaOffsetMonitor-assembly-sohu-0.1.0.jar com.sohu.kafka.offsetapp.OffsetGetterWeb > logs/monitor.log 2>&1 &
