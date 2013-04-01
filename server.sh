#!/usr/bin/env bash
# coding=utf-8
# Author: zhouguoqing
# Data:   2013-04-1
# version:1.0.1
######################## 
if [ -z $1 ]
then     
    YDAY=`date -d "1 day ago" +%Y%m%d`
    else  
    YDAY=$1    
fi
echo "$YDAY run twister"  
storm jar twister-0.0.1-jar-with-dependencies.jar com.twister.topology.TwisterTopology TwisterTopology $YDAY >$YDAY.out&
echo "ok!"