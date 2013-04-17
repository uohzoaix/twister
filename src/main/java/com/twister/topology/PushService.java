package com.twister.topology;

import java.net.InetAddress;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Queues;
import com.mongodb.BasicDBObject;
import com.mongodb.ServerAddress;
import com.twister.jzmq.PushSer;
import com.twister.nio.client.DisplaySpoutIp;
import com.twister.nio.server.NioTcpServer;
import com.twister.nio.server.NioUdpServer;
import com.twister.storage.mongo.MongoManager;
import com.twister.utils.AppsConfig;
import com.twister.utils.Constants;

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
 
 * 
 */

public class PushService {
	public static Logger logger = LoggerFactory.getLogger(PushService.class);
	public static String[] Tport = AppsConfig.getInstance().getValue("tcp.spout.port").split(",");
	public static String[] Uport = AppsConfig.getInstance().getValue("udp.spout.port").split(",");
	public static String[] Pport = AppsConfig.getInstance().getValue("pull.spout.port").split(",");
	public static ConcurrentLinkedQueue<Runnable> th = new ConcurrentLinkedQueue<Runnable>();

	public static void run() throws Exception {

		MongoManager mgo = MongoManager.getInstance();
		List<ServerAddress> ls = mgo.getAddr();
		for (ServerAddress serverAddress : ls) {
			System.out.println("mongodb " + serverAddress.getHost() + ":" + serverAddress.getPort() + " mapi");
		}
		mgo.remove(Constants.SpoutTable, new BasicDBObject().append("desc", "spout"));
		BlockingQueue<String> queues = Queues.newLinkedBlockingQueue(Constants.QueueSize);
		String localip = InetAddress.getLocalHost().getHostAddress();
		System.out.println("service ip " + localip);
		// tcp receive lines
		for (int i = 0; i < Tport.length; i++) {
			int port = Integer.valueOf(Tport[i]);
			NioTcpServer tcpServer = new NioTcpServer(queues, port);
			tcpServer.run();
			th.offer(tcpServer);
		}
		// udp receive lines
		for (int i = 0; i < Uport.length; i++) {
			int port = Integer.valueOf(Uport[i]);
			NioUdpServer udpServer = new NioUdpServer(queues, port);
			udpServer.run();
			th.offer(udpServer);
		}

		// push/pull to spout

		for (int i = 0; i < Pport.length; i++) {
			int port = Integer.valueOf(Pport[i]);
			PushSer push = new PushSer(queues, port);
			Thread th1 = new Thread(push);
			th1.setDaemon(true);
			th1.start();
			th.offer(push);
		}
		DisplaySpoutIp.dispclient();

	}

	public static void main(String[] args) {
		try {
			run();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}