package com.twister.jzmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

public class PullCli implements Runnable {

	/**客户端recive消息的代码
	 * @param args
	 */
	private static final Logger logger = LoggerFactory.getLogger(PullCli.class);
	private final int port;
	private final String host;
	private ZMQ.Context context;
	private ZMQ.Socket receiver;
	private String protocl;
	public boolean run = true;

	public PullCli(String ip, int port) {
		this.host = ip;
		this.port = port;
		this.open();
	}

	public void open() {
		try {
			context = ZMQ.context(1);
			receiver = context.socket(ZMQ.PULL);
			protocl = String.format("tcp://%s:%d", this.host, this.port);
			receiver.connect(protocl);
			run = true;
			logger.info("push/pull已准备好 " + protocl);
		} catch (Exception e) {
			e.printStackTrace();
			logger.info(e.getStackTrace().toString());
		}

	}

	public ZMQ.Context getContext() {
		return context;
	}

	public ZMQ.Socket getReceiver() {
		return receiver;
	}

	public String getProtocl() {
		return protocl;
	}

	public void setProtocl(String protocl) {
		this.protocl = protocl;
	}

	public String recv() {
		if (receiver == null) {
			this.open();
		}
		String msg = new String(receiver.recv(0));
		// logger.info(msg);
		return msg;
	}

	public void close() {
		receiver.close();
		context.term();
		run = false;
	}

	/**
	 * is open connect
	 */
	@Override
	public void run() {
		if (receiver == null) {
			this.open();
		}
	}

	// public static void main(String[] args) {
	// PullCli pull = new PullCli("127.0.0.1", 5557);
	// System.out.println("pull");
	// Thread thr1 = new Thread(pull, "pullcli");
	// thr1.setDaemon(false);
	// thr1.start();
	// while (true) {
	// pull.recv();
	// }
	//
	// }

}
