package com.twister.topology;


import com.twister.bolt.SimpleBolt1;
import com.twister.bolt.SimpleBolt2;
import com.twister.spout.SimpleSpout;
import com.twister.utils.StormRunner;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class AccessLogTopology
{
    public static void main( String[] args ) throws Exception
    {
        TopologyBuilder topologyBuilder=new TopologyBuilder();
        topologyBuilder.setSpout("accessLogSpout", new SimpleSpout(),1);
        topologyBuilder.setBolt("simple-bilt",new SimpleBolt1(), 3).shuffleGrouping("simple-spout");
        topologyBuilder.setBolt("wordcounter", new SimpleBolt2(),3).fieldsGrouping("simple-bilt", new Fields("info"));
        Config config=new Config();
        config.setDebug(true);
         
        if(null!=args&&args.length>0){
            //使用集群模式运行
            config.setNumWorkers(1);
            StormRunner.runTopologyCluster(args[0], config, topologyBuilder.createTopology());
        }
        else{
            //使用本地模式运行
            config.setMaxTaskParallelism(1);
            StormRunner.runTopologyLocally("local-jstormer",config,topologyBuilder.createTopology(), 1);
                  
                        
        }
    }
}