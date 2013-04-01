#!/usr/bin/env bash
# coding=utf-8
# Author: zhouguoqing
# Data:   2013-04-1
# version:1.0.1
######################## 
if [ -z $1 ]
then     
    YDAY=`date -d "0 day ago" +%Y%m%d`
else  
    YDAY=$1    
fi
echo "$YDAY run twister"  
java -cp classes -classpath ./target/twister-0.0.1-jar-with-dependencies.jar com.twister.nio.client.SendNioTcpClient 10.103.23.63 10236 /v3/data/syslog/hour/20130401/access_$YDAY >$YDAY.out
echo "ok"