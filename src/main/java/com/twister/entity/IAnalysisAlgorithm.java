package com.twister.entity;

import java.util.Map;

import org.bson.BSONObject;

/**
 * 计算
 * 
 * @author zhouguoqing
 * 
 */

public interface IAnalysisAlgorithm<T> {


	public String objectToJson();

	public <T> T fromJson(String json, Class<T> c);
	
	public void assess_request_time(int response_code, long request_time);
	
	/**
	 * 
	 * @param obj
	 *            本次计算的对象 old obj
	 * @return obj 两个对象的计算完后的 this
	 */
	public void calculate(T oldobj);
}