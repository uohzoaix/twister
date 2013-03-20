package com.twister.nio.log;

import java.io.Serializable;

import com.twister.utils.JacksonUtils;

public abstract class AbstractAnalysis<T> implements Serializable,IAnalysisAlgorithm<T> {
	
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
		this.key=ukey;		
	}

	@Override
	public String objectToJson() {
		return JacksonUtils.objectToJson(this);		 
	}
	
	@Override
	public <T> T fromJson(String json, Class<T> c) {
		 return JacksonUtils.jsonToObject(json,c);	 	 
	}

 
 
	
}
