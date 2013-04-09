package com.twister.storage;
 

public interface Icache<T> {

 
	public <T> T restoreObjectFromJson(String key, String jsonStr, Class<T> valueType);
	

	public String storeObjectToJson(String key, T obj);
	
 
	
}
