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

public interface IAnalysisAlgorithm<T>{	 
	public String getKey();
	public void setKey(String ukey);	
	public String objectToJson();
	public <T> T fromJson(String json, Class<T> c);
	@SuppressWarnings("rawtypes")
	public Map<String,Object> objectToMap();	 
	public void assess_request_time(int response_code, long request_time);	
	 
	/**
	 * 
	 * @param obj 本次计算的对象 old obj	 
	 * @return obj 两个对象的计算完后的 this obj
	 */
	public T calculate(T obj);
}