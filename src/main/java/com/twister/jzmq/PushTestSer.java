package com.twister.jzmq;


import java.util.concurrent.BlockingQueue;


import com.google.common.collect.Queues;
import com.twister.utils.Common;
import com.twister.utils.Constants;

public class PushTestSer {
	public static void main(String[] args) {
		int port = 10239;
		BlockingQueue<String> qu = Queues.newLinkedBlockingQueue(Constants.QueueSize);
		for (int i = 0; i < 10; i++) {
			qu.offer(Common.AccessYouku);
			qu.offer(Common.AccessTudou);
		}
		PushSer push = new PushSer(qu, port);
		System.out.println("pushserver");
		Thread thr = new Thread(push, "pushSer");
		thr.setDaemon(false);
		thr.start();
	}

}
