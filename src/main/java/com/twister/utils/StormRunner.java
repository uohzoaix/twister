package com.twister.utils;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SubmitOptions;
import backtype.storm.utils.Utils;

public final class StormRunner {

    private static final int MILLIS_IN_SEC = 1000;
    
    private StormRunner() {
    }    
    
    public static void runTopologyLocally(String topologyName,Config stormConf, StormTopology topology, int runtimeInSeconds)
            throws InterruptedException {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, stormConf, topology);
        Thread.sleep((long) runtimeInSeconds * MILLIS_IN_SEC);
        cluster.killTopology(topologyName);
        cluster.shutdown();
    }
    
    public static void runTopologyCluster(String topologyName,Config stormConf, StormTopology topology) 
    		throws Exception {
    	StormSubmitter.submitTopology(topologyName, stormConf, topology, null);
    }
    
    public static void runTopologyCluster(String topologyName, Config stormConf, StormTopology topology, SubmitOptions opts) 
    		throws Exception {
    	StormSubmitter.submitTopology(topologyName, stormConf, topology, opts);
    }    
    
}
