package com.twister.jzmq;

import java.net.InetAddress;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import com.google.common.collect.Queues;
import com.mongodb.BasicDBObject;
import com.twister.nio.server.MoniterQueue;
import com.twister.storage.mongo.MongoManager;
import com.twister.utils.Common;
import com.twister.utils.Constants;

public class PushSer implements Runnable {

	/**服务器给客户端发送消息的代码
	 * @param args
	 */
	private static final Logger logger = LoggerFactory.getLogger(PushSer.class);
	private final Queue<String> queue;
	private final int port;
	private MongoManager mgo;
	private ZMQ.Context context;
	private ZMQ.Socket sender;
	public boolean run = true;
	public MoniterQueue moniter;

	public PushSer(final Queue<String> shareQueue, int port) {
		this.queue = shareQueue;
		this.port = port;
		this.open();
	}

	public void open() {
		try {
			mgo = MongoManager.getInstance();
			context = ZMQ.context(1);
			sender = context.socket(ZMQ.PUSH);
			String protocl = String.format("tcp://*:%d", this.port);
			sender.bind(protocl);
			String localip = InetAddress.getLocalHost().getHostAddress();
			String dts = Common.createDataStr();
			String serinfo = "tcp:" + localip + ":" + port + " " + dts;
			BasicDBObject sermap = new BasicDBObject();
			sermap.put("ip", localip);
			sermap.put("port", port);
			sermap.put("kind", "push");
			sermap.put("desc", "spout");
			sermap.put("day", dts);
			mgo.insertOrUpdate(Constants.SpoutTable, sermap, sermap);
			run = true;
			moniter = new MoniterQueue(this.queue, "MoniterQueue");
			logger.info("push/pull服务端已准备好 " + serinfo);
		} catch (Exception e) {
			e.printStackTrace();
			logger.info(e.getStackTrace().toString());
		}

	}

	public void close() {
		sender.close();
		context.term();
		run = false;
	}

	@Override
	public void run() {
		while (run) {
			try {
				moniter.show();
				String line = queue.poll();
				String txt = String.format("%s", line);
				if (txt != null && txt.length() > 10 && Common.Ipv4.matcher(txt).find()) {
					boolean sd = sender.send(txt.getBytes(), 0);
					// logger.info(txt + "   " + sd);
				}
			} catch (Exception e) {
				e.printStackTrace();
				logger.info(e.getStackTrace().toString());
			}

		}
	}

	// public static void main(String[] args) {
	// Queue<String> qu = Queues.newConcurrentLinkedQueue();
	// for (int i = 0; i < 10; i++) {
	// qu.offer(Common.AccessYouku);
	// qu.offer(Common.AccessTudou);
	// }
	// PushSer push = new PushSer(qu, 5557);
	// System.out.println("push");
	// Thread thr = new Thread(push, "pushSer");
	// thr.setDaemon(false);
	// thr.start();
	// }
}
