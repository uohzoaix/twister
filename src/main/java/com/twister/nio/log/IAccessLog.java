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
	
	/**
	 * 1.先初始化conf
	 */
	public void initSettings();
	
	/**
	 * check input line
	 * 
	 * @param line
	 * @return
	 */
	public boolean checkAccessLog(String line);
	
	/**
	 * 先用正则表达式匹配line 有错误时候用 split方式
	 * 
	 * @param line
	 * @return
	 */
	public ArrayList<String> parseLog(String line);
	
	public String repr();
	
	public String valToString(String delm);
	
	public ArrayList<String> logMatcher(String line);
	
	public ArrayList<String> logSplit(String str, String regex);
	
	public String sublogString(String str, int start, int end);
	
	/**
	 * format matcher or split array value
	 * 
	 * @param alog
	 * @return
	 */
	public ArrayList<String> formatAccessLog(ArrayList<String> alog);
	
	/**
	 * expands to object ,is build new object
	 * 
	 * @param itr
	 */
	public void logExpandsToObject(ArrayList<String> itr);
	
	public Logger getLogger();
}