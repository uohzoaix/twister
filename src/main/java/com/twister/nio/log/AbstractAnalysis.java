package com.twister.nio.log;

import com.twister.utils.JacksonUtils;

public abstract class AbstractAnalysis<T> implements IAnalysisAlgorithm<T> {
    public String key;    
	@Override
	public String getKey() {
		 
		return key;
	}

	@Override
	public void setKey(String ukey) {
		this.key=key;		
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
