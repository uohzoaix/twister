package com.twister.simple;

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.io.input.TailerListenerAdapter;

import com.twister.storage.cache.EhcacheMap;

public class Tail2 {
	/**
	 * The log file tailer
	 */
	
	public static Logger logger = LoggerFactory.getLogger(Tail2.class);
	
	/**
	 * Command-line launcher
	 * 
	 * @return
	 */
	
	public static void main(String[] args) {
		
		String[] infiles = new String[] { "/tmp/tailer.log" };
		if (args.length < 1) {
			System.out.println("Usage: Tail <filename>");
			args = infiles;
		}
		File file = new File(args[0]);
		TailerListener listener = new MyTailerListener();
		Tailer tailer = new Tailer(file, listener, 100, true);
		Thread tt = new Thread(tailer);
		tt.start();
		
		EhcacheMap<String, Integer> map = new EhcacheMap<String, Integer>("EhcacheMap");
		map.put("11", 3);
		System.out.println(map.get("11"));
		
	}
	
}
