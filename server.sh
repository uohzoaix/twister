#!/usr/bin/env bash
# coding=utf-8
# Author: zhouguoqing
# Data:   2013-04-1
# version:1.0.1
######################## 
DIR=$(cd $(dirname $0); pwd) 
cd $DIR
echo $DIR
if [ -z $1 ]
then     
    YDAY=`date -d "0 day ago" +%Y%m%d`
    else  
    YDAY=$1    
fi
echo "$YDAY service"
java -server -Xmx768m -Dstorm.home=/opt/storm-0.8.2 -Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib -cp ..:/opt/java/default/lib:/opt/storm-0.8.2:/opt/storm-0.8.2/lib:/opt/storm-0.8.2/libext -classpath twister-0.0.1-jar-with-dependencies.jar com.twister.nio.server.MainService $YDAY >> $YDAY.out &
#echo "topology"
#storm jar $DIR/twister-0.0.1-jar-with-dependencies.jar com.twister.topology.TwisterTopology TwisterTopology $YDAY >>$YDAY.out &
echo "ok!"
