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
echo "$YDAY run twister"
storm jar $DIR/twister-0.0.1-jar-with-dependencies.jar com.twister.nio.server.MainService $YDAY >>$YDAY.out &
storm jar $DIR/twister-0.0.1-jar-with-dependencies.jar com.twister.topology.TwisterTopology TwisterTopology $YDAY >>$YDAY.out &
echo "ok!"