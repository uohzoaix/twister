package com.twister.nio.log;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.twister.utils.Common;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.twister.nio.log.AccessLogAnalysis;
 
/**
 * 统计类
 * 
 * @author zhouguoqing
 * 
 */ 

public class AccessLogAlgorithm {	 
	
	private AccessLogAnalysis alog ;
	
	private static Map<String, Values> _result = new ConcurrentHashMap<String, Values>();
	private static long rs_cnt = 0l;
	
	public AccessLogAlgorithm() {
	}
	
	public AccessLogAlgorithm(AccessLogAnalysis lastAlog ,AccessLogAnalysis currentAlog) {
		 
	}
 
	public synchronized AccessLogAnalysis calculate(AccessLogAnalysis lastAlog ,AccessLogAnalysis currentAlog) {
		
		return alog;		
	}
	
 
	
//	public static void main(String vs[]) {
//		 AccessLogStatics als = new AccessLogStatics();
//		 als.calculate("aaa", 200, 4, 15);
//		 als.calculate("aaa", 408, 1, 100);
//		 als.calculate("aaa", 200, 105, 80);
//		 System.out.println(als.toString());
//	}
	
}