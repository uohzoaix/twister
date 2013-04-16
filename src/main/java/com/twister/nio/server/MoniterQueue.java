package com.twister.nio.server;

import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twister.utils.Constants;

/**
 * MoniterQueue mq= new MoniterQueue(queue, "MoniterQueue");
 * Thread th = new Thread(mq, "MoniterQueue");
 *  th.setDaemon(true);
	th.start();
 * @author guoqing
 *
 */
public class MoniterQueue {
	private static final Logger logger = LoggerFactory.getLogger(MoniterQueue.class.getName());
	private final Queue<String> q;
	private String queueName = "";
	private long begtime;
	private long lasttime;

	public MoniterQueue(Queue<String> a, String queueName) {
		q = a;
		begtime = System.currentTimeMillis();
		lasttime = System.currentTimeMillis();
	}

	public void show() {
		lasttime = System.currentTimeMillis();
		if (begtime + Constants.SyncInterval < lasttime) {
			begtime = lasttime;
			logger.info(queueName + lasttime + " size " + q.size());
		}
	}

}
