package com.twister.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.TridentTopology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.twister.bolt.AccessLogGroup;
import com.twister.bolt.AccessLogStatis;
import com.twister.bolt.AccessLogShuffle;
import com.twister.spout.NioTcpServerSpout;
import com.twister.spout.NioUdpServerSpout;
//import com.twister.spout.TextFileSpout;
//import com.twister.spout.TailFileSpout;

/**
 * <p>
 * Description : TwisterTopology <br>
 * usage: Topology 不支持事务，没有批量提交 storm jar
 * target/twister-0.0.1-jar-with-dependencies.jar
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
		// TextFileSpout textSpout = new
		// TextFileSpout("src/main/resources/words.txt");
		// TailFileSpout Tailspout = new
		// TailFileSpout("src/main/resources/words.txt");
		
		// use nio tcp good
		NioTcpServerSpout tcpspout = new NioTcpServerSpout(Tport); // 10236
		NioUdpServerSpout udpspout = new NioUdpServerSpout(Uport); // 10237
		
		// 收集日志分发
		builder.setSpout("tcpTwisterSpout", tcpspout);
		builder.setSpout("udpTwisterSpout", udpspout);
		// Initial filter
		// 随机分组，平衡计算结点 String id, IRichBolt, thread num
		builder.setBolt("shuffleBolt", new AccessLogShuffle(), 5).shuffleGrouping("udpTwisterSpout")
				.shuffleGrouping("tcpTwisterSpout");
		
		// group bolt
		builder.setBolt("fieldsGroupBolt", new AccessLogGroup(), 5).fieldsGrouping("shuffleBolt",
				new Fields("ukey", "AccessLogAnalysis"));
		
		// 汇总,统计结点 bolt,入redis内存
		builder.setBolt("statisBolt", new AccessLogStatis(), 5).globalGrouping("fieldsGroupBolt");
		
		// config
		Config conf = new Config();
		conf.setDebug(true);
		
		if (null != args && args.length > 0) {
			// 使用集群模式运行
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			logger.debug("集群模式运行 " + "udp port:" + Uport + " tcp port:" + Tport);
		} else {
			// 使用本地模式运行
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			logger.debug("本地模式运行 " + "udp port:" + Uport + " tcp port:" + Tport);
			cluster.submitTopology("twister", conf, builder.createTopology());
			Thread.sleep(2 * 1000);
			// cluster.shutdown();
			
		}
	}
}