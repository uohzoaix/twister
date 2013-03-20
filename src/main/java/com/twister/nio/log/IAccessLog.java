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
	public static final String RealLogEntryPattern = "^([\\d.]+) \"(\\d{4}\\-\\d{2}\\-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\+\\d{2}:\\d{2})\" (\\S+) \"(\\S+)\" \"(.+)\" \"(\\S*)\" ([\\d]+) ([\\d]+) ([\\d]+\\.[\\d]+)\\s\"(.*)\"(.*)";
	// syslog-ng add per
	public static final Pattern syslogExtPer = Pattern
			.compile("^(\\w+\\s+\\d+\\s+\\d{2}:\\d{2}:\\d{2})\\s+\\w([a-zA-Z_0-9\\-]+) ");
	public static final Pattern RealLogPattern = Pattern.compile(RealLogEntryPattern);
	public static final Pattern Ipv4 = Pattern.compile("\\d+\\.\\d+\\.\\d+\\.\\d+");
	public static final Pattern Ipv6 = Pattern.compile("\\S*:\\S*:\\S*:\\S*:\\S*:\\S*:\\S*:\\S*:");
	
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