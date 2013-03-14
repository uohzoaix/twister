package com.twister.nio.log;
 
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
public class AccessLog extends AbstractAccessLog<AccessLog> {
	
	private static final long serialVersionUID = 4224713360551345643L;
	
	private static Logger LOGR = LoggerFactory.getLogger(AccessLog.class);
 
	
	public AccessLog() {
	}
	
	public AccessLog(String line) {
		this.initSettings();
		ArrayList<String> alog = parseLog(line);
		this.logExpandsToObject(alog);
	}
 	
	@Override
	public String toString() {
		return super.toString();
	}
	
	@Override
	public Logger getLogger() {
		return LOGR;
	}
	
	
}
