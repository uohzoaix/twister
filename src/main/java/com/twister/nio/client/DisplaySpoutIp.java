package com.twister.nio.client;

import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.twister.storage.mongo.MongoManager;
import com.twister.utils.Constants;

public class DisplaySpoutIp {

	/**
	 * @param args
	 */
	public static void dispclient() {
		MongoManager mgo = MongoManager.getInstance();

		List<Map> list = mgo.query(Constants.SpoutTable, new BasicDBObject().append("desc", "spout"));
		System.out.println(Constants.SpoutTable + " rows " + list.size());
		for (Map m : list) {
			m.remove("_id");
			System.out.println(m.toString());
		}
		long ct = mgo.getCollection(Constants.ApiStatisTable).count();
		System.out.println(Constants.ApiStatisTable + " rows " + ct);
		System.out.println("findOne " + mgo.getCollection(Constants.ApiStatisTable).findOne());
	}

	public static void main(String[] args) {
		dispclient();
	}

}
