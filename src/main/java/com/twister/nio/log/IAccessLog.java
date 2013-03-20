package com.twister.nio.log;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Pattern;

import org.slf4j.Logger;

import com.twister.utils.Common;

public interface IAccessLog<T> {
	// accesslog
	public static final String RealLogEntryPattern = Common.RealLogEntryPattern;
	// syslog-ng add per
	public static final Pattern syslogExtPer = Common.syslogExtPer;
	public static final Pattern RealLogPattern = Common.RealLogPattern;
	public static final Pattern Ipv4 = Common.Ipv4;
	public static final Pattern Ipv6 = Common.Ipv6;
	
	public static final Charset charSet = Charset.forName("UTF-8");
	public static final String SPACE = " "; // 空格键
	public static final ArrayList<Map<String, Serializable>> uriRegex = Common.getUriRegexConf(Common.UriRegexFile);
	
	public void initSettings();
	
	public boolean checkAccessLog(String line);
	
	public ArrayList<String> parseLog(String line);
	
	public String repr();
	
	public String valToString(String delm);
	
	public ArrayList<String> logMatcher(String line);
	
	public ArrayList<String> logSplit(String str, String regex);
	
	public String sublogString(String str, int start, int end);
	
	public ArrayList<String> formatAccessLog(ArrayList<String> alog);
	
	public void logExpandsToObject(ArrayList<String> itr);
	
	public Logger getLogger();
}