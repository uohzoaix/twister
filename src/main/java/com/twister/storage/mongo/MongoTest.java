package com.twister.storage.mongo;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MongoTest {
	
	public MongoManager mr = MongoManager.getInstance();
	public String table = "test";
	public DBCollection coll = mr.getCollection("test");

	public void query() {
		Map keyMap = new HashMap();
		keyMap.put("user", "jimmy1");
		keyMap.put("fileName", "test1");
		// List<Map> list = mr.query(table, keyMap);
		List<Map> list = mr.queryAll(table);
		for (Map m : list) {
			System.out.println(m);
		}
	}
	
	public void insertOrUpdateTest() {
		String fname = "test6";
		Map updateQuery = new HashMap();
		updateQuery.put("fileName", fname);
		
		Map newObj = new HashMap();
		newObj.put("fileName", fname);
		newObj.put("date", "20130207");
		newObj.put("time", "112705");
		newObj.put("user", "ffff");
		newObj.put("id", "555");
		
		mr.insertOrUpdate(table, updateQuery, newObj);
	}
	
	public void insertOrUpdate() {
		String fname = "test4";
		Map newDocument = new HashMap();
		newDocument.put("fileName", fname);
		boolean isNew = true;
		if (mr.query(table, newDocument).size() > 0) {
			isNew = false;
		}
		newDocument.put("date", "20130207");
		newDocument.put("time", "112705");
		newDocument.put("user", "jimmy2");
		newDocument.put("id", "22222");
		System.out.println("isNew::" + isNew);
		if (isNew) {
			mr.insert(table, newDocument);
		} else {
			mr.update(table, new BasicDBObject().append("fileName", fname), newDocument);
		}
		
	}
	
	public void deleteAll() {
		// mr.delete("", "");
		Map d = new HashMap();
		d.put("fileName", "DLGO62.gul");
		mr.delete(table, d);
	}
	


	public void insertMemo() throws Exception {
		DBCollection coll = mr.getCollection("test");
		BasicDBObject doc = new BasicDBObject();
		doc.put("title", "one meeting");
		doc.put("palce", "one meeting");
		doc.put("time", new Date());
		coll.insert(doc);
	}

	public void getMemo() throws Exception {
		DBCollection coll = mr.getCollection("test");
		BasicDBObject obj = (BasicDBObject) coll.findOne();
		System.out.println(obj);
	}

	public void queryMemo() throws Exception {
		DBCollection coll = mr.getCollection("test");
		BasicDBObject obj = new BasicDBObject();
		obj.put("title", "one meeting");
		DBCursor cursor = coll.find(obj);
		while (cursor.hasNext()) {
			Date date = (Date) cursor.next().get("time");
			System.out.println(date.toLocaleString());
		}
		cursor.close();
	}

	public void delete() throws Exception {
		BasicDBObject query = new BasicDBObject();
		query.put("title", "one meeting");
		// 找到并且删除，并返回删除的对象
		DBObject removeObj = coll.findAndRemove(query);
		System.out.println(removeObj);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		MongoTest test = new MongoTest();
		System.out.println(test.mr.getCollections());
		// test.insertOrUpdateTest();

		BasicDBObject doc = new BasicDBObject();
		doc.put("title", "test");
		doc.put("cnt", 3);

		Map mp=doc.toMap();


		test.mr.getCollection("test").remove(new BasicDBObject().append("title", "test"));
		test.mr.getCollection("test");
		test.mr.insertOrUpdate("test", new BasicDBObject().append("title", "test"), doc);
		List<Map> list = test.mr.queryAll("test");
		for (Map m : list) {
			System.out.println(m);
		}
		// test.deleteAll();
		// test.query();
		

	}
}
