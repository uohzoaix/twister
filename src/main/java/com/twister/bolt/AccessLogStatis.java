package com.twister.bolt;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.mongodb.BasicDBObject;
import com.twister.entity.AccessLogAnalysis;
import com.twister.storage.AccessLogCacheManager;
import com.twister.storage.cache.EhcacheMap;
import com.twister.storage.mongo.MongoManager;
import com.twister.storage.redis.JedisManager;
import com.twister.storage.redis.JedisManager.JedisExpireHelps;
import com.twister.utils.AppsConfig;
import com.twister.utils.Constants;
import com.twister.utils.JacksonUtils;

/**
 * 直接发给redisbolt汇总
 * 
 * @author guoqing
 * 
 */
public class AccessLogStatis extends BaseRichBolt {

	private final Logger LOGR = LoggerFactory.getLogger(AccessLogStatis.class.getName());
	private static final long serialVersionUID = 2246728833921545687L;
	private Integer taskid;
	private String name;
	private OutputCollector collector;
	private AccessLogCacheManager alogManager;
	private MongoManager mgo;
	private EhcacheMap<String, AccessLogAnalysis> ehcache; // 缓存的内空有限，只能存6分钟
	// private EhcacheMap<String, Integer> hashCounter;
	private Long GLOB = 0l;
	private String tips = "";
	// 更新频率值
	private long frequency = Constants.FREQUENCY;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.taskid = context.getThisTaskId();
		// conf/ehcache.xml
		alogManager = new AccessLogCacheManager();
		ehcache = alogManager.getMapEhcache();
		mgo = MongoManager.getInstance();
		// JedisManager jm = JedisManager.getInstance();
		// this.hashCounter = alogManager.getMapCounter();
		frequency = AppsConfig.getInstance().getValue("cntpv.frequency") != "" ? Long.valueOf(AppsConfig.getInstance().getValue("cntpv.frequency")) : frequency;
		tips = String.format("componentId name :%s,task id :%s ", this.name, this.taskid);
		LOGR.info(tips);

	}

	@Override
	public void execute(Tuple input) {
		// this tuple 提取次数
		// GLOB += 1;
		try {

			// pojo,key为试想拼合的字款 ,time也可以分成2
			// ukey=ver#time#rely#server
			// 0#20120613#10:01:00#0
			String ukey = input.getStringByField("ukey");
			// LOGR.info(tips + String.format(GLOB + " %s", ukey));
			AccessLogAnalysis logalys = (AccessLogAnalysis) input.getValueByField("AccessLogAnalysis");
			logalys.setTxid(String.valueOf(taskid));
			// 初始可能并发修改
			// Utils.sleep((int) Math.random() * 1000);
			synchronized (this) {
				if (ehcache.containsKey(ukey)) {
					AccessLogAnalysis clog = (AccessLogAnalysis) ehcache.get(ukey);
					clog.calculate(logalys); // 在对象里算
					// 覆盖原来的对象
					clog.setTxid(String.valueOf(taskid));
					ehcache.put(ukey, clog);
					logalys = (AccessLogAnalysis) clog.clone();
				} else {
					ehcache.put(ukey, logalys);
				}
			}
			// 发射累积的统计结果
			collector.emit(new Values(ukey, logalys));
			// dumperValue(ukey, logalys);

			// 更新次数
			// if (hashCounter.containsKey(ukey)) {
			// int cnt = hashCounter.get(ukey).intValue();
			// cnt += 1;
			// hashCounter.put(ukey, cnt);
			// } else {
			// hashCounter.put(ukey, 1);
			// }

			// dumper统计结果 to redis
			if (ehcache.containsKey(ukey)) {
				AccessLogAnalysis rlt = (AccessLogAnalysis) ehcache.get(ukey);
				// 更新频率值
				if (rlt.getCnt_pv() >= frequency) {
					// save to
					saveMongo(ukey, rlt);
				}
				// LOGR.info(tips + String.format(GLOB + " result:%s,ehcache=%s ", ukey, rlt.getCnt_pv()));
			}

			// 通过ack操作确认这个tuple被成功处理
			collector.ack(input);
			// LOGR.info(tips + " statis==cntRow===" + GLOB);
		} catch (Exception e) {
			e.printStackTrace();
			LOGR.error(e.getStackTrace().toString());
		}
	}

	private synchronized void dumperValue(final String ukey, final AccessLogAnalysis rlt) {
		String tmp = ukey + "\t" + rlt.getCnt_pv() + "\t" + rlt.getTxid() + "\r\n";
		FileWriter fw;
		try {
			fw = new FileWriter("AccessLogStatisDumper.txt", true);
			fw.write(tmp);
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private synchronized void saveMongo(final String ukey, final AccessLogAnalysis rlt) {
		if (ukey.length() > 0 && rlt != null) {
			Map<String, String> mp = rlt.splitUkey(0, ukey, "#");
			if (mp.size() > 0) {
				BasicDBObject queryobj = new BasicDBObject();
				queryobj.putAll(mp);
				mgo.insertOrUpdate(Constants.ApiStatisTable, queryobj, rlt.toBasicDBObject());
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		LOGR.info(String.format("AccessLogStatis OutputFieldsDeclarer is %s", "AccessLog"));
		declarer.declare(new Fields("ukey", "AccessLogAnalysis"));
	}

	@Override
	public void cleanup() {
		// hashCounter.clear();
		ehcache.clear();

	}

}
