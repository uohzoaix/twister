package com.twister.io.input;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Pattern;

import org.slf4j.Logger;

import com.twister.utils.Common;

public interface IAccessLog {
	// accesslog
	public static final String RealLogEntryPattern = "^([\\d.]+) \"(\\d{4}\\-\\d{2}\\-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\+\\d{2}:\\d{2})\" (\\S+) \"(\\S+)\" \"(.+)\" \"(\\S*)\" ([\\d]+) ([\\d]+) ([\\d]+\\.[\\d]+) \"(.*)\"(.*)";
	// syslog-ng add per
	public static Pattern syslogExtPer = Pattern.compile("^(\\w+\\s+\\d+\\s+\\d{2}:\\d{2}:\\d{2})\\s+\\w(\\d+)");
	public static Pattern RealLogPattern = Pattern.compile(RealLogEntryPattern);
	
	public Charset charSet = Charset.forName("UTF-8");
	public final String SPACE = " "; // 空格键
	public static ArrayList<Map<String, Serializable>> uriRegex = Common.getUriRegexConf(Common.UriRegexFile);
	
	public void initSettings();
	
	public boolean checkAccessLog(String line);
	
	public ArrayList<String> parseLog(String line);
	
	public String repr();
	
	public String valToString(String delm);
	
	public ArrayList<String> logMatcher(String line);
	
	public ArrayList<String> logSplit(String str, String regex);
	
	public String sublogString(String str, int start, int end);
	
	@SuppressWarnings("rawtypes")
	public ArrayList<String> formatAccessLog(ArrayList<String> alog);
	
	@SuppressWarnings("rawtypes")
	public void logExpandsToObject(ArrayList<String> itr);
	
	public Logger getLogger();
}