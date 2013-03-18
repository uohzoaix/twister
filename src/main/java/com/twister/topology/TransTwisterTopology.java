package com.twister.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twister.bolt.trans.AccessLogShuffleTransBolt;
import com.twister.bolt.trans.AccessLogTransStatis;
import com.twister.spout.trans.SyslogNioUdpTransSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;
 
 

/**
 * <p>
 * Description : TwisterTopology <br>
 * usage: Topology
 * 支持事务，批量提交
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

public class TransTwisterTopology {
	public static Logger logger = LoggerFactory.getLogger(TransTwisterTopology.class);
	public static int Tport = 10236;
	public static int Uport = 10237;	
	public static void main(String[] args) throws Exception {
		
		 TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("transTwister", "AlogUdpSpout", new SyslogNioUdpTransSpout(Uport),10);
		 builder.setBolt("shuffleTransBolt", new AccessLogShuffleTransBolt(), 20).shuffleGrouping("AlogUdpSpout");
		 builder.setBolt("statisTransBolt", new AccessLogTransStatis(), 20).fieldsGrouping("users-splitter","alogs", new Fields("txid","ukey","AccessLogAnalysis"));
		 
		 // config
		Config conf = new Config();
		conf.setDebug(true);
		
		if (null != args && args.length > 0) {
			// 使用集群模式运行
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, builder.buildTopology());
			logger.debug("集群模式运行 " +"udp port:" +Uport +" tcp port:"+Tport);
		} else {
			// 使用本地模式运行			 
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			logger.debug("本地模式运行 " +"udp port:" +Uport +" tcp port:"+Tport);
			cluster.submitTopology("transTwisterTopology", conf, builder.buildTopology());			
			Thread.sleep(2 * 1000);
			// cluster.shutdown();
			 
			
		}
	}
}