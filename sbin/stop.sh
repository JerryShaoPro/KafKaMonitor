#!/bin/bash

pid=`ps aux | grep KafkaMonitor | grep -v "grep" | awk '{print $2}'`

kill -9 $pid
