package com.twister.bolt;

import java.util.Map;
import java.util.List;
import com.twister.bolt.RedisBolt.OnDynamicConfigurationListener;

public class SimpleRedisBolt extends RedisBolt implements
		OnDynamicConfigurationListener {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4645779884411581495L;
	private static final String CHANNEL = "market";

	public SimpleRedisBolt() {		
		super(CHANNEL);
	}

	@Override
	protected void setupNonSerializableAttributes() {
		super.setupNonSerializableAttributes();
		setupDynamicConfiguration(this);
	}

	@Override
	public boolean publishMessage(String string) {		 
		String jsonString = (String) currentTuple.getValue(0);
		if (jsonString == null) {
			return false;
		}
		try {			 
			publish(CHANNEL, jsonString);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}

	@Override
	public void onConfigurationChange(String conf) {
	}

}
