package com.twister.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.twister.bolt.AccessLogGroup;
import com.twister.bolt.AccessLogStatis;
import com.twister.bolt.AccessLogShuffle;
import com.twister.spout.NioTcpServerSpout;
import com.twister.spout.NioUdpServerSpout;
import com.twister.utils.AppsConfig;
import com.twister.utils.FileUtils;

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
		String tmpfile = AppsConfig.getInstance().getValue("save.spoutIpPort.file");
		FileUtils.writeFile(tmpfile, "", false); // clean tmpfile
		TopologyBuilder builder = new TopologyBuilder();
		// setup your spout
		// TextAccessFileSpout textSpout = new
		// TextAccessFileSpout("src/main/resources/words.txt");
		// TailFileSpout Tailspout = new
		// TailFileSpout("src/main/resources/words.txt");
		// Initial filter
		// 随机分组，平衡计算结点 String id, IRichBolt, thread num
		BoltDeclarer bde = builder.setBolt("shuffleBolt", new AccessLogShuffle(), 30);
		String tcp = "";
		String udp = "";
		for (int i = 0; i < Tport.length; i++) {
			// use nio tcp good
			int port = Integer.valueOf(Tport[i]);
			// 收集日志分发
			SpoutDeclarer sd = builder.setSpout("tcpTwisterSpout" + port, new NioTcpServerSpout(port));
			logger.info("tcpTwisterSpout" + port);
			tcp += " " + port;
			bde.shuffleGrouping("tcpTwisterSpout" + port);
		}
		for (int i = 0; i < Uport.length; i++) {
			// use nio udp
			int port = Integer.valueOf(Uport[i]);
			// 收集日志分发
			SpoutDeclarer sd = builder.setSpout("udpTwisterSpout" + port, new NioUdpServerSpout(port));
			logger.info("udpTwisterSpout" + port);
			udp += " " + port;
			bde.shuffleGrouping("udpTwisterSpout" + port);
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
		builder.setBolt("statisBolt", new AccessLogStatis(), 60).globalGrouping("fieldsGroupBolt");
		
		// config
		Config conf = new Config();
		conf.setDebug(true);
		
		if (null != args && args.length > 0) {
			// 使用集群模式运行
			conf.setNumWorkers(30);
			StormSubmitter.submitTopology("TwisterTopology", conf, builder.createTopology());
			logger.info("StormCluster*****" + "udp port:" + udp + " tcp port:" + tcp);
		} else {
			// 使用本地模式运行
			conf.setMaxTaskParallelism(5);
			LocalCluster cluster = new LocalCluster();
			logger.info("LocalCluster***** " + "udp port:" + udp + " tcp port:" + tcp);
			cluster.submitTopology("twister", conf, builder.createTopology());
			Thread.sleep(2 * 1000);
			// cluster.shutdown();
			
		}
	}
}