package com.twister.entity;

import java.io.Serializable;

import com.twister.utils.JacksonUtils;

public abstract class AbstractAnalysis<T> implements Serializable, Cloneable, IAnalysisAlgorithm<T> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 666708219120833466L;
	
	public String key;
	
	@Override
	public String getKey() {
		
		return key;
	}
	
	@Override
	public void setKey(String ukey) {
		this.key = ukey;
	}
	
	@Override
	public String objectToJson() {
		return JacksonUtils.objectToJson(this);
	}
	
	@Override
	public <T> T fromJson(String json, Class<T> c) {
		return JacksonUtils.jsonToObject(json, c);
	}
	
	@Override
	public Object clone() {
		Object o = null;
		try {
			o = (T) super.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return o;
	}
	
}
