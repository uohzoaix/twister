#!/usr/bin/env bash
# coding=utf-8
# Author: zhouguoqing
# Data:   2013-04-1
# version:1.0.1
######################## 
DIR=$(cd $(dirname $0); pwd)
cd $DIR
if [ -z $1 ]
then     
    File=/opt/logs/nginx/access/log
else  
    File=$1
fi 
if [ -z $2 ]
then     
    YDAY=`date -d "0 day ago" +%Y%m%d`
else  
    YDAY=$2
fi
echo "$YDAY run twister"  
#  /v3/data/syslog/day/$YDAY/access_$YDAY 
#echo "syslog /v3/data/syslog/day/20130411/access_20130411"
echo "java -cp classes -classpath $DIR/twister-0.0.1-jar-with-dependencies.jar com.twister.nio.client.SendNioTcpClient $File" 
java -cp classes -classpath $DIR/twister-0.0.1-jar-with-dependencies.jar com.twister.nio.client.DisplaySpoutIp
java -cp classes -classpath $DIR/twister-0.0.1-jar-with-dependencies.jar com.twister.nio.client.SendNioTcpClient $File > $YDAY.out &
echo "ok"