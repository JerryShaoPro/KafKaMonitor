#!/bin/bash

pid=`ps aux | grep KafkaOffsetMonitor | grep -v "grep" | awk '{print $2}'`

kill -9 $pid
