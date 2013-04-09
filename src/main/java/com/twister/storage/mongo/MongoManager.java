package com.twister.storage.mongo;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Properties;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.WriteResult;
import com.twister.utils.AppsConfig;


/*shell
 * mongo -u admin -p admin 127.0.0.1:27017/db
 * <pre>

* A MongoDB client with internal connection pooling. For most applications, you should have one MongoClient instance
* for the entire JVM.
* <p>
* The following are equivalent, and all connect to the local database running on the default port:
* <pre>
* MongoClient mongoClient1 = new MongoClient();
* MongoClient mongoClient1 = new MongoClient("localhost");
* MongoClient mongoClient2 = new MongoClient("localhost", 27017);
* MongoClient mongoClient4 = new MongoClient(new ServerAddress("localhost"));
* MongoClient mongoClient5 = new MongoClient(new ServerAddress("localhost"), new MongoClientOptions.Builder().build());
* </pre>
* <p>
* You can connect to a
* <a href="http://www.mongodb.org/display/DOCS/Replica+Sets">replica set</a> using the Java driver by passing
* a ServerAddress list to the MongoClient constructor. For example:
* <pre>
* MongoClient mongoClient = new MongoClient(Arrays.asList(
*   new ServerAddress("localhost", 27017),
*   new ServerAddress("localhost", 27018),
*   new ServerAddress("localhost", 27019)));
* </pre>
* You can connect to a sharded cluster using the same constructor.  MongoClient will auto-detect whether the servers are
* a list of replica set members or a list of mongos servers.
*/
public class MongoManager {
	
	/**
	 * retrieve All the Collections Name
	 * 
	 * @return List
	 * @throws UnknownHostException
	 */

	private DB db = null;
	private MongoClient mongoClient = null;
	private DBCollection collection = null;
	private List<ServerAddress> addr = new ArrayList<ServerAddress>();
	private String database = "mapi";
	private static MongoManager SingleInstance = null;

	public MongoManager() {
		getConnect();
	}

	public DB getDb() {
		if (db == null) {
			// reset
			SingleInstance = null;
			MongoManager.getInstance();
		}
		return db;
	}

	public DB getDb(String database) {
		if (db == null) {
			// reset
			SingleInstance = null;
			this.database = database;
			MongoManager.getInstance();
		}
		db = mongoClient.getDB(database);
		return db;
	}

	public void setDb(DB db) {
		this.db = db;
	}

	public List<ServerAddress> getAddr() {
		return addr;
	}

	public void setAddr(List<ServerAddress> addr) {
		this.addr = addr;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	/**
	 * load Properties conf/mongo.properties
	 * database=mapi,default 127.0.0.1:27017
	 * @return MongoManager
	 */
	public synchronized static MongoManager getInstance() {
		if (SingleInstance == null) {
			SingleInstance = new MongoManager();
		}
		return SingleInstance;
	}

	/**
	 *  an abstract class that represents a logical database on a server   
	 * @return databases
	 */
	public DB getConnect() {
		try {
			Properties prop = new Properties();
			if (SingleInstance == null) {
				prop = AppsConfig.loadProperties("conf/mongo.properties");
			}
			int port = Integer.valueOf(prop.getProperty("port", "27017"));
			String[] ips = prop.getProperty("hosts", "127.0.0.1").split(",");
			database = prop.getProperty("database", database);
			if (SingleInstance == null) {
				addr.clear();
				for (String hs : ips) {
					String[] hostPortPair = hs.split(":");
					if (hostPortPair.length > 1) {
						try {
							port = Integer.valueOf(hostPortPair[1]);
						} catch (NumberFormatException e) {
						}
					}
					addr.add(new ServerAddress(hs, port));
					mongoClient = new MongoClient(addr);
				}
			}
		} catch (Exception e) {
			addr.clear();
			try {
				addr.add(new ServerAddress("127.0.0.1", 27017));
				mongoClient = new MongoClient(addr);
			} catch (UnknownHostException e1) {
				e1.printStackTrace();
			}
		}
		db = mongoClient.getDB(database);
		collection = db.getCollection(database);
		return db;
	}

	public void dropDatabase() {
		db.dropDatabase();
	}


	public DBCollection getCollection(String TableName) {
		try {
			collection = db.getCollection(TableName);
		} catch (Exception e) {
			collection = getInstance().getConnect().getCollection(TableName);
		}
		return collection;
	}

	public List<String> getCollections() {
		Set<String> colls = getInstance().getConnect().getCollectionNames();
		List<String> collectionList = new ArrayList<String>();
		for (String table : colls) {
			collectionList.add(table);
		}
		return collectionList;
	}
	

	
	/**
	 * find All data By condition
	 * 
	 * @param TableName
	 * @param keyMap
	 * @return DBCursor
	 */
	private DBCursor queryByKey(String TableName, Map<String, Object> keyMap) {
		BasicDBObject ref = new BasicDBObject();
		ref.putAll(keyMap);
		return getCollection(TableName).find(ref);
	}
	
	/**
	 * find All data
	 * 
	 * @param TableName
	 * @return
	 */
	public List queryAll(String TableName) {
		DBCursor dbCursor = getCollection(TableName).find();
		List<Map> list = new ArrayList<Map>();
		while (dbCursor.hasNext()) {
			DBObject obj = dbCursor.next();
			list.add(obj.toMap());
		}
		return list;
	}
	
	/**
	 * find By condition
	 * 
	 * @param TableName
	 * @param keyMap
	 *            Map<key,value>
	 * @return
	 */
	public List query(String TableName, Map keyMap) {
		return DBCursorToList(queryByKey(TableName, keyMap));
	}

	
	private List DBCursorToList(DBCursor dbCursor) {
		List<Map> list = new ArrayList<Map>();
		while (dbCursor.hasNext()) {
			DBObject obj = dbCursor.next();
			list.add(obj.toMap());
		}
		return list;
	}
	
	/**
	 * 
	 * @param TableName
	 * @param BasicDBObject info
	 */
	public boolean insert(String TableName, Map info) {
		WriteResult wr = getCollection(TableName).insert(new BasicDBObject(info));
		if (wr.getN() > 0) {
			return true;
		}
		return false;
	}

	/**
	 * 
	 * @param TableName
	 * @param BasicDBObject info
	 */
	public void delete(String TableName, Map info) {
		BasicDBObject delObj = new BasicDBObject();
		delObj.putAll(info);
		getCollection(TableName).remove(delObj);
	}

	/**
	 * 
	 * @param TableName
	 * @param BasicDBObject info
	 */
	public void remove(String TableName, BasicDBObject delObj) {
		getCollection(TableName).remove(delObj);
	}

	/**
	 * not insert
	 * @param TableName
	 * @param updateQuery
	 * @param updateObject
	 */
	public void update(String TableName, Map updateQuery, Map updateObject) {
		BasicDBObject newObj = new BasicDBObject();
		BasicDBObject oldObj = new BasicDBObject();
		newObj.putAll(updateObject);
		oldObj.putAll(updateQuery);
		getCollection(TableName).update(oldObj, newObj);
	}

	/**
	 * is first query >0 ,insert
	 * @param TableName
	 * @param updateQuery
	 * @param updateObject
	 * @return boolean
	 */
	public boolean insertOrUpdate(String TableName, Map updateQuery, Map object) {
		try {
			if (query(TableName, updateQuery).size() > 0) {
				update(TableName, updateQuery, object);
			} else {
				insert(TableName, object);
			}
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	/**
	 * 
	 * @param tableName
	 * @param queryBasicDBObject
	 * @param updateBasicDBObject
	 * @return
	 */
	
	public boolean insertOrUpdate(String tableName, BasicDBObject queryBasicDBObject, BasicDBObject updateBasicDBObject) {
		DBCollection coll = getCollection(tableName);
		try {
			coll.update(queryBasicDBObject, updateBasicDBObject, true, false);
		} catch (Exception e) {
			return false;
		}
		return true;
	}


}
