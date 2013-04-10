package com.twister.topology;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.mongodb.BasicDBObject;
import com.mongodb.ServerAddress;
import com.twister.bolt.AccessLogGroup;
import com.twister.bolt.AccessLogStatis;
import com.twister.bolt.AccessLogShuffle;
import com.twister.spout.NioTcpServerSpout;
import com.twister.spout.NioUdpServerSpout;
import com.twister.storage.mongo.MongoManager;
import com.twister.utils.AppsConfig;
import com.twister.utils.Constants;


//import com.twister.spout.TextAccessFileSpout;
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
	 
	public static String[] Tport = AppsConfig.getInstance().getValue("tcp.spout.port").split(",");
	public static String[] Uport = AppsConfig.getInstance().getValue("udp.spout.port").split(",");

	public static void main(String[] args) throws Exception {
		MongoManager mgo = MongoManager.getInstance();
		List<ServerAddress> ls = mgo.getAddr();
		for (ServerAddress serverAddress : ls) {
			System.out.println("mongodb " + serverAddress.getHost() + ":" + serverAddress.getPort() + " mapi");
		}

		mgo.remove(Constants.SpoutTable, new BasicDBObject().append("desc", "spout"));
		TopologyBuilder builder = new TopologyBuilder();
		// setup your spout
		// TextAccessFileSpout textSpout = new
		// TextAccessFileSpout("src/main/resources/words.txt");
		// TailFileSpout Tailspout = new
		// TailFileSpout("src/main/resources/words.txt");
		// Initial filter
		// 随机分组，平衡计算结点 String id, IRichBolt, thread num
		BoltDeclarer bde = builder.setBolt("shuffleBolt", new AccessLogShuffle(), 30);

		for (int i = 0; i < Tport.length; i++) {
			// use nio tcp good
			int port = Integer.valueOf(Tport[i]);
			// 收集日志分发
			String title = "tcpTwisterSpout_" + port;
			SpoutDeclarer sd = builder.setSpout(title, new NioTcpServerSpout(port));
			bde.shuffleGrouping(title);
			logger.info(title);
		}

		for (int i = 0; i < Uport.length; i++) {
			// use nio udp
			int port = Integer.valueOf(Uport[i]);
			// 收集日志分发
			String title = "udpTwisterSpout_" + port;
			SpoutDeclarer sd = builder.setSpout(title, new NioUdpServerSpout(port));
			bde.shuffleGrouping(title);
			logger.info(title);
		}

		// NioTcpServerSpout tcpspout = new NioTcpServerSpout(10236); // 10236
		// NioUdpServerSpout udpspout = new NioUdpServerSpout(10237); // 10237
		// 收集日志分发
		// builder.setSpout("tcpTwisterSpout", tcpspout);
		// builder.setSpout("udpTwisterSpout", udpspout);
		// Initial filter
		// 随机分组，平衡计算结点 String id, IRichBolt, thread num
		// builder.setBolt("shuffleBolt", new
		// AccessLogShuffle(),30).shuffleGrouping("udpTwisterSpout").shuffleGrouping("tcpTwisterSpout");
		
		// group bolt
		builder.setBolt("fieldsGroupBolt", new AccessLogGroup(), 30).fieldsGrouping("shuffleBolt",
				new Fields("ukey", "AccessLogAnalysis"));
		
		// 汇总,统计结点 bolt,入redis内存
		builder.setBolt("statisBolt", new AccessLogStatis(), 60).fieldsGrouping("fieldsGroupBolt",
				new Fields("ukey", "AccessLogAnalysis"));
		
		// config
		Config conf = new Config();
		conf.setDebug(true);
		
		if (null != args && args.length > 0) {
			// 使用集群模式运行
			conf.setNumWorkers(30);
			StormSubmitter.submitTopology("TwisterTopology", conf, builder.createTopology());
			logger.info("StormCluster");
		} else {
			// 使用本地模式运行
			conf.setMaxTaskParallelism(5);
			LocalCluster cluster = new LocalCluster();
			logger.info("LocalCluster");
			cluster.submitTopology("twister", conf, builder.createTopology());
			Thread.sleep(2 * 1000);
			// cluster.shutdown();
			
		}
	}
}