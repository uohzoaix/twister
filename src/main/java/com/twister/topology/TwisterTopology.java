package com.twister.topology;

import java.net.InetAddress;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.google.common.collect.Queues;
import com.mongodb.BasicDBObject;
import com.mongodb.ServerAddress;
import com.twister.bolt.AccessLogGroup;
import com.twister.bolt.AccessLogStatis;
import com.twister.bolt.AccessLogShuffle;
import com.twister.jzmq.PushSer;
import com.twister.nio.server.NioTcpServer;
import com.twister.nio.server.NioUdpServer;
//import com.twister.spout.NioTcpServerSpout;
//import com.twister.spout.NioUdpServerSpout;
import com.twister.spout.PullSpout;
import com.twister.storage.mongo.MongoManager;
import com.twister.utils.AppsConfig;
import com.twister.utils.Constants;

//import com.twister.spout.TextAccessFileSpout;
//import com.twister.spout.TailFileSpout;

/**
 * Queue 
------------ 
1.ArrayDeque, （数组双端队列） 
2.PriorityQueue, （优先级队列） 
3.ConcurrentLinkedQueue, （基于链表的并发队列） 
4.DelayQueue, （延期阻塞队列）（阻塞队列实现了BlockingQueue接口） 
5.ArrayBlockingQueue, （基于数组的并发阻塞队列） 
6.LinkedBlockingQueue, （基于链表的FIFO阻塞队列） 
7.LinkedBlockingDeque, （基于链表的FIFO双端阻塞队列） 
8.PriorityBlockingQueue, （带优先级的无界阻塞队列） 
9.SynchronousQueue （并发同步阻塞队列 size=0） 
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
	public static String[] Pport = AppsConfig.getInstance().getValue("pull.spout.port").split(",");

	public static void main(String[] args) throws Exception {
		MongoManager mgo = MongoManager.getInstance();
		List<ServerAddress> ls = mgo.getAddr();
		for (ServerAddress serverAddress : ls) {
			System.out.println("mongodb " + serverAddress.getHost() + ":" + serverAddress.getPort() + " mapi");
		}

		mgo.remove(Constants.SpoutTable, new BasicDBObject().append("desc", "spout"));
		// newConcurrentLinkedQueue LinkedBlockingQueue newArrayBlockingQueue
		Queue<String> queues = Queues.newArrayBlockingQueue(Constants.QueueSize);
		String localip = InetAddress.getLocalHost().getHostAddress();
		TopologyBuilder builder = new TopologyBuilder();
		BoltDeclarer bde = builder.setBolt("shuffleBolt", new AccessLogShuffle(), Constants.ShuffleBolt);
		// tcp receive lines
		for (int i = 0; i < Tport.length; i++) {
			int port = Integer.valueOf(Tport[i]);
			NioTcpServer tcpServer = new NioTcpServer(queues, port);
			tcpServer.run();
		}
		// udp receive lines
		for (int i = 0; i < Uport.length; i++) {
			int port = Integer.valueOf(Uport[i]);
			NioUdpServer udpServer = new NioUdpServer(queues, port);
			udpServer.run();
		}

		// push/pull to spout
		ExecutorService pushService = Executors.newCachedThreadPool();
		for (int i = 0; i < Pport.length; i++) {
			int port = Integer.valueOf(Pport[i]);
			PushSer push = new PushSer(queues, port);
			pushService.submit(push);
			pushService.submit(push);
			pushService.submit(push);
			String title = "push_pull_spout_" + port;
			SpoutDeclarer sd = builder.setSpout(title, new PullSpout(localip, port), Constants.PullSpout);
			bde.shuffleGrouping(title);
			logger.info(title);
		}
		logger.info(pushService.toString());
		// setup your spout
		// TextAccessFileSpout textSpout = new
		// TextAccessFileSpout("src/main/resources/words.txt");
		// TailFileSpout Tailspout = new
		// TailFileSpout("src/main/resources/words.txt");
		// Initial filter
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
		builder.setBolt("fieldsGroupBolt", new AccessLogGroup(), Constants.FieldsGroupBolt).fieldsGrouping("shuffleBolt",
				new Fields("ukey", "AccessLogAnalysis"));

		// 汇总,统计结点 bolt,入redis内存
		builder.setBolt("statisBolt", new AccessLogStatis(), Constants.StatisBolt).fieldsGrouping("fieldsGroupBolt", new Fields("ukey", "AccessLogAnalysis"));

		// config
		Config conf = new Config();
		conf.setDebug(true);

		if (null != args && args.length > 0) {
			// 使用集群模式运行
			conf.setNumWorkers(Constants.NumWorkers);
			StormSubmitter.submitTopology("TwisterTopology", conf, builder.createTopology());
			pushService.shutdownNow();
			logger.info("StormCluster");
		} else {
			// 使用本地模式运行
			conf.setMaxTaskParallelism(Constants.NumWorkers);
			LocalCluster cluster = new LocalCluster();
			logger.info("LocalCluster");
			cluster.submitTopology("twister", conf, builder.createTopology());
			Thread.sleep(2 * 1000);
			pushService.shutdownNow();
			// cluster.shutdown();

		}
	}
}