package com.twister.topology;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import com.twister.bolt.LowercaseBolt;
import com.twister.bolt.WordCountBolt;
import com.twister.bolt.WordExtractorBolt;
import com.twister.spout.TailFileSpout;
import com.twister.spout.TextFileSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * <p>
 * Description : WordCountTopology <br>
 * usage: Topology
 * </p>
 * 
 * <pre></pre>
 * 
 * @author guoqing
 * @see TopologyBuilder 
 * 
 */

public class WordCountTopology {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		TextFileSpout textSpout = new TextFileSpout("src/main/resources/words.txt");
				
		builder.setSpout("spouter", textSpout);
		builder.setBolt("extract", new WordExtractorBolt(), 3).shuffleGrouping("spouter");
		builder.setBolt("lower", new LowercaseBolt(), 3).shuffleGrouping("extract");
		builder.setBolt("count", new WordCountBolt(), 3).fieldsGrouping("lower", new Fields("word"));
		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			// 使用集群模式运行
			conf.setNumWorkers(5);
			StormSubmitter.submitTopology(args[0], conf,builder.createTopology());
		} else {
			// 使用本地模式运行
			conf.setMaxTaskParallelism(3);
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("wordcount", conf,builder.createTopology());
			Thread.sleep(10 * 1000);
			localCluster.shutdown();
		}
	}
}
