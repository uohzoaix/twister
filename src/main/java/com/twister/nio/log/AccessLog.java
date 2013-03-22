package com.twister.nio.log;

import java.util.ArrayList;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
@JsonIgnoreProperties(value = { "logger", "LOGR", "serialVersionUID", "proxyIp", "timeCalendar", "timeDate" })
public class AccessLog extends AbstractAccessLog<AccessLog> implements Cloneable {
	
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
	
	@Override
	public Object clone() {
		Object o = null;
		try {
			o = (AccessLog) super.clone(); // Object 中的clone()识别出你要复制的是哪一个对象。
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return o;
	}
}
