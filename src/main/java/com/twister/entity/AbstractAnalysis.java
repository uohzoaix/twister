package com.twister.entity;

import java.io.Serializable;

import com.twister.utils.JacksonUtils;

public abstract class AbstractAnalysis<T> implements Serializable, Cloneable, IAnalysisAlgorithm<T> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 666708219120833466L;
	
	public String ukey;

	
	public String getUkey() {
		return ukey;
	}

	public void setUkey(String ukey) {
		this.ukey = ukey;
	}

	@Override
	public String objectToJson() {
		return JacksonUtils.objectToJson(this);
	}
	
	@Override
	public <T> T fromJson(String json, Class<T> c) {
		return JacksonUtils.jsonToObject(json, c);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Object clone() {
		Object o = null;
		try {
			o = (T) super.clone();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return o;
	}
	
}
