package com.twister.simple;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.io.input.TailerListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Fields;

import com.twister.nio.log.LogFileTailer;
import com.twister.utils.Common;

public class Tail3 extends TailerListenerAdapter {
	/**
	 * The log file tailer test
	 */
	private final LogFileTailer tailer;
	public static Logger logger = LoggerFactory.getLogger(Tail3.class);
	private boolean startAtBeginning = false;
	
	/**
	 * Creates a new Tail instance to follow the specified file
	 */
	public Tail3(String filename, boolean startAtBegin) {
		this.startAtBeginning = startAtBegin;
		tailer = new LogFileTailer(new File(filename), 1000, startAtBeginning);
		tailer.addLogFileTailerListener(this);
		tailer.start();
	}
	
	public Tail3(File file, boolean startAtBegin) {
		this.startAtBeginning = startAtBegin;
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
	
	@Override
	public void handle(String line) {
		logger.info(line);
	}
	
	/**
	 * Command-line launcher
	 */
	public static void main(String[] args) {
		// echo "time `date -d "0 day ago" +%Y%m%d%S`" >>/tmp/tailer.log
		// String[] infiles = new String[] { "/tmp/tailer.log" };
		// if (args.length < 1) {
		// System.out.println("Usage: Tail <filename>");
		// args = infiles;
		// //System.exit(0);
		// }
		// Tail tail = new Tail(args[0],false);
		try {
			String datestr = "2012-03-11 12:56:90";
			SimpleDateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Long t;
			 
			t = dateFormat1.parse(datestr).getTime();
			
			//t = t / 1000;
			System.out.println(t +"  "+Common.longMinute(datestr) );
			
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String dt = dateFormat.format(new Date(t));
			System.out.println(dt);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
