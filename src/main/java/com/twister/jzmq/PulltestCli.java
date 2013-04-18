package com.twister.jzmq;

import java.io.File;


import com.twister.utils.Constants;

public class PulltestCli {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		// Parse options.
		String host = "127.0.0.1";
		int port = 10239;
		File logfile = new File(Constants.nginxAccess);
		System.out.println("Usage : " + PullCli.class.getName() + " <host> <port>");
		if (args.length == 2) {
			host = args[0];
			port = Integer.valueOf(args[1]);
		}
		PullCli pull = new PullCli(host, port);
		System.out.println("pull tcp://" + host + ":" + port);
		Thread thr1 = new Thread(pull, "pullcli");
		thr1.setDaemon(false);
		thr1.start();
		while (true) {
			String rc = pull.recv();
			System.out.println(rc);
		}
	}

}
