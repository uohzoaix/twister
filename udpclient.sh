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
#java -cp classes -classpath $DIR/twister-0.0.1-jar-with-dependencies.jar com.twister.nio.client.DisplaySpoutIp
java -Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib -cp ..:/opt/java/default/lib:/opt/storm-0.8.2:/opt/storm-0.8.2/lib:/opt/storm-0.8.2/libext -classpath twister-0.0.1-jar-with-dependencies.jar com.twister.nio.client.DisplaySpoutIp
java -Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib -cp ..:/opt/java/default/lib:/opt/storm-0.8.2:/opt/storm-0.8.2/lib:/opt/storm-0.8.2/libext -classpath twister-0.0.1-jar-with-dependencies.jar com.twister.nio.client.SendNioUdpClient $File >> $YDAY.out &
echo "ok"