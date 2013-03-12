package com.twister.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.twister.bolt.AccessLogStatistic;
import com.twister.bolt.AccessLogShuffle;
import com.twister.bolt.RedisStorageBolt;
import com.twister.spout.SyslogNioTcpSpout;
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
		long startTime = System.currentTimeMillis();
		TopologyBuilder builder = new TopologyBuilder();
		
		// setup your spout
		// TextFileSpout spout = new
		// TextFileSpout("src/main/resources/words.txt");
		
		// TailFileSpout spout = new
		// TailFileSpout("src/main/resources/words.txt");
		
		// SyslogUdpSpout spout = new
		// SyslogUdpSpout(10237,InetAddress.getLocalHost());
		// SyslogTcpSpout spout = new SyslogTcpSpout(10236);
	     SyslogNioTcpSpout tcpspout = new SyslogNioTcpSpout(Tport);
		
		SyslogNioUdpSpout udpspout = new SyslogNioUdpSpout(Uport);
		//收集日志分发
		builder.setSpout("tcpTwister", tcpspout,10);
		builder.setSpout("udpTwister", udpspout,10);
		
		// Initial filter
		//随机分组，平衡计算结点 String id, IRichBolt, thread num
		builder.setBolt("alogShuffle", new AccessLogShuffle(), 20).shuffleGrouping("tcpTwister").shuffleGrouping("udpTwister");
		 
		//统计结点 bolt
		builder.setBolt("statistic", new AccessLogStatistic(), 20).fieldsGrouping("alogShuffle",new Fields("AccessLog"));
		//汇总结点及入redis内存 bolt
		
		//builder.setBolt("storage", new RedisStorageBolt(), 20).fieldsGrouping("statistic",new Fields("AccessLog"));
		// config
		Config conf = new Config();
		conf.setDebug(true);
		
		if (null != args && args.length > 0) {
			// 使用集群模式运行
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			logger.debug("集群模式运行 " +"udp port:" +Uport +" tcp port:"+Tport);
		} else {
			// 使用本地模式运行
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("twister", conf, builder.createTopology());
			logger.debug("本地模式运行 " +"udp port:" +Uport +" tcp port:"+Tport);
			Thread.sleep(10 * 1000);
			// cluster.shutdown();
			
		}
	}
}