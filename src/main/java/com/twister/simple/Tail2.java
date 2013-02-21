package com.twister.simple;

import java.io.File;
 
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.io.input.TailerListenerAdapter;

public class Tail2 extends TailerListenerAdapter {
	/**
	 * The log file tailer
	 */
	 
	public static Logger logger = LoggerFactory.getLogger(Tail2.class);
	

    @Override
    public void handle(String line) {           
           logger.info("File handle new line");
           System.out.print(line);    
    }

    @Override
    public void fileRotated() {
        logger.info("File was rotated or rename");
    }
    
    public Tail2(){
    	
    }

 
 
	/**
	 * Command-line launcher
	 */
	public static void main(String[] args) {	
		String[] infiles = new String[] { "/tmp/tailer.log" };
		if (args.length < 1) {
			System.out.println("Usage: Tail <filename>");
			args=infiles;
		}		    
		 
		File file=new File(args[0]);
		TailerListener listener = new Tail2();
		Tailer tailer =Tailer.create(file, listener, 100, true);
		tailer.run();		
		 	 
	}
	
	
}
