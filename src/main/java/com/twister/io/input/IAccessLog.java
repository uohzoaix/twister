package com.twister.io.input;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.mapreduce.Mapper.Context;

public interface IAccessLog<T> {
	// accesslog
	public static final String RealLogEntryPattern = "^([\\d.]+) \"(\\d{4}\\-\\d{2}\\-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\+\\d{2}:\\d{2})\" (\\S+) \"(\\S+)\" \"(.+)\" \"(\\S*)\" ([\\d]+) ([\\d]+) ([\\d]+\\.[\\d]+) \"(.*)\"(.*).*";
	// syslog-ng add per
	public static Pattern syslogExtPer = Pattern.compile("^(\\w+\\s+\\d+\\s+\\d{2}:\\d{2}:\\d{2})\\s+a(\\d+)");
	public static Pattern RealLogPattern = Pattern.compile(RealLogEntryPattern);
	
	public Charset charSet = Charset.forName("UTF-8");
	public final String SPACE=" ";  //空格键
	 
	public boolean checkAccessLog(String line);
	public ArrayList<String> parseLog(String line);
	public String repr();
	 
	public ArrayList<String> logMatcher(String line);
	
	public ArrayList<String> logSplit(String str, String regex);
	
	@SuppressWarnings("rawtypes")
	public ArrayList<String> formatAccessLog(ArrayList<String> alog); 
	 
	@SuppressWarnings("rawtypes") 
	public void logToObject(ArrayList<String> itr);
	
	@SuppressWarnings("rawtypes")
	public void logExpands(ArrayList<String> itr);
	

}