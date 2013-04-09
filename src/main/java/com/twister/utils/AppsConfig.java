package com.twister.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Properties;

public class AppsConfig {
	private static String local_settings = "conf/local_settings.xml";
	private static String settings = "conf/settings.xml";
	private static Properties props = null;
	private static AppsConfig singleInstance = null;

	// 懒汉式单例类
	public synchronized static AppsConfig getInstance() {
		if (singleInstance == null) {
			singleInstance = new AppsConfig();
		}
		return singleInstance;
	}

	private AppsConfig() {
		try {
			props = new Properties();
			InputStream is = AppsConfig.class.getClassLoader().getResourceAsStream(settings);
			props.loadFromXML(is);

			InputStream is2 = AppsConfig.class.getClassLoader().getResourceAsStream(local_settings);
			props.loadFromXML(is2);

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public String getValue(String name) {
		return props.getProperty(name);
	}

	public static Properties loadProperties(String confFile) {
		Properties prop = new Properties();
		try {
			// load a properties file
			InputStream is = AppsConfig.class.getClassLoader().getResourceAsStream(confFile);
			prop.load(is);
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return prop;
	}

	public static void main(String[] args) {
		// System.out.println(AppsConfig.getInstance().getValue("apimsdb.driver") + " " + AppsConfig.getInstance().getValue("hadoop.user"));
	}

}