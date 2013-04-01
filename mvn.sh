#!/usr/bin/env bash
# coding=utf-8
# Author: zhouguoqing
# Data:   2013-04-10
# version:1.0.1
######################## 
if [ -z $1 ]
then     
    YDAY=`date -d "1 day ago" +%Y%m%d`
else  
    YDAY=$1    
fi
echo "$YDAY mvn clean package"  
mvn clean package
echo "mvn ok!"
cp ./target/twister-0.0.1-jar-with-dependencies.jar .
exit 0

