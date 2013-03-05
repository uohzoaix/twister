package com.twister.simple;

import java.io.File;

import org.apache.commons.io.input.TailerListenerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twister.io.input.LogFileTailer;

public class Tail3 extends TailerListenerAdapter {
	/**
	 * The log file tailer test
	 */
	private LogFileTailer tailer;
	public static Logger logger = LoggerFactory.getLogger(Tail3.class);
	private boolean startAtBeginning = false;
	/**
	 * Creates a new Tail instance to follow the specified file
	 */
	public Tail3(String filename,boolean startAtBegin) {
		this.startAtBeginning=startAtBegin;
		tailer = new LogFileTailer(new File(filename), 1000, startAtBeginning);
		tailer.addLogFileTailerListener(this);
		tailer.start();
	}

	public Tail3(File file,boolean startAtBegin) {
		this.startAtBeginning=startAtBegin;
		tailer = new LogFileTailer(file, 1000, startAtBeginning);
		tailer.addLogFileTailerListener(this);
		tailer.start();
	}

	/**
	 * A new line has been added to the tailed log file
	 * 
	 * @param line
	 *            The new line that has been added to the tailed log file
	 */

	public void handle(String line) {
		logger.info(line);
	}

	/**
	 * Command-line launcher
	 */
	public static void main(String[] args) {
		// echo "time `date -d "0 day ago" +%Y%m%d%S`" >>/tmp/tailer.log
//		String[] infiles = new String[] { "/tmp/tailer.log" };
//		if (args.length < 1) {
//			System.out.println("Usage: Tail <filename>");
//			args = infiles;
//			//System.exit(0);
//		}
//		Tail tail = new Tail(args[0],false);

	}

}
