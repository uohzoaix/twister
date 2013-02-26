package com.twister.topology;

import java.net.InetAddress;

import com.twister.bolt.SimpleRedisBolt;
import com.twister.bolt.WordCountBolt;
import com.twister.bolt.WordExtractorBolt;
import com.twister.spout.SyslogUdpSpout;
import com.twister.spout.TailFileSpout;
import com.twister.spout.TextFileSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * <p>
 * Description : TwisterTopology <br>
 * usage: Topology 
 *  
 * storm jar target/twister-0.0.1-jar-with-dependencies.jar com.twister.topology.TwisterTopology
 * *
 * </p>
 * 
 * <pre>
 * http://blog.sina.com.cn/s/blog_5ca749810101c34u.html
 * </pre>
 * 
 * @author guoqing
 * @see TopologyBuilder
 * @see https://github.com/nathanmarz/storm-contrib
 * 
 */

public class TwisterTopology {
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		// setup your spout
		//TextFileSpout spout = new TextFileSpout("src/main/resources/words.txt");
		
		//TailFileSpout spout = new TailFileSpout("src/main/resources/words.txt");	
		
		SyslogUdpSpout spout = new SyslogUdpSpout(10234,InetAddress.getLocalHost());	
		
		builder.setSpout("twister", spout);		 
		// Initial filter		 
		builder.setBolt("extract", new WordExtractorBolt(), 3).shuffleGrouping("twister");
		// bolt
		builder.setBolt("wordcounter", new WordCountBolt(), 3).fieldsGrouping("extract", new Fields("word"));
		 
		// config
		Config conf = new Config();
		conf.setDebug(true);

		if (null != args && args.length > 0) {
			// 使用集群模式运行
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf,builder.createTopology());
		} else {
			// 使用本地模式运行
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();			 
			cluster.submitTopology("twister", conf, builder.createTopology());
			Thread.sleep(10 * 1000);
			//cluster.shutdown();

		}
	}
}