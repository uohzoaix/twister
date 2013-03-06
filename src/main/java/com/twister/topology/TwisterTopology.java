package com.twister.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.twister.bolt.WordCountBolt;
import com.twister.bolt.WordExtractorBolt;
import com.twister.spout.SyslogNioUdpSpout;

/**
 * <p>
 * Description : TwisterTopology <br>
 * usage: Topology
 * 
 * storm jar target/twister-0.0.1-jar-with-dependencies.jar
 * com.twister.topology.TwisterTopology *
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
	public static Logger logger = LoggerFactory.getLogger(TwisterTopology.class);
	public static int Tport = 10236;
	public static int Uport = 10237;
	
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		
		// setup your spout
		// TextFileSpout spout = new
		// TextFileSpout("src/main/resources/words.txt");
		
		// TailFileSpout spout = new
		// TailFileSpout("src/main/resources/words.txt");
		
		// SyslogUdpSpout spout = new
		// SyslogUdpSpout(10237,InetAddress.getLocalHost());
		// SyslogTcpSpout spout = new SyslogTcpSpout(10236);
		// SyslogNioTcpSpout spout = new SyslogNioTcpSpout(10236);
		
		SyslogNioUdpSpout spout = new SyslogNioUdpSpout(Uport);
		
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
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			logger.debug("集群模式运行 ");
		} else {
			// 使用本地模式运行
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("twister", conf, builder.createTopology());
			logger.debug("本地模式运行 " + Uport);
			Thread.sleep(10 * 1000);
			// cluster.shutdown();
			
		}
	}
}