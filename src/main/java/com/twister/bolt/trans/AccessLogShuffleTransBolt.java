package com.twister.bolt.trans;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.twister.nio.log.AccessLog;
import com.twister.nio.log.AccessLogAnalysis;

/**
 * 将分析结果随机分发到Bolt的任务，保证每个任务获得相等数量的tuple
 * 
 * @author guoqing
 * 
 */
public class AccessLogShuffleTransBolt implements IBasicBolt {
	
	private static final long serialVersionUID = 1896733498701080791L;
	public static Logger logger = LoggerFactory.getLogger(AccessLogShuffleTransBolt.class);
	BasicOutputCollector collector;
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		System.out.println("shuffTransBolt declareOutputFields");
		declarer.declareStream("alogs", new Fields("txid", "ukey", "AccessLogAnalysis"));
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		System.out.println("shuffTransBolt getComponentConfiguration");
		return null;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		System.out.println("shuffTransBolt prepare ");
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		this.collector = collector;
		try {
			TransactionAttempt tx = (TransactionAttempt) input.getValueByField("txid");
			String ukey = input.getStringByField("ukey");
			AccessLog alog = (AccessLog) input.getValueByField("AccessLog");
			// 转化成少的pojo由code算出cnt_error等,不累加直接发过去
			AccessLogAnalysis logalys = new AccessLogAnalysis(alog.outKey(), alog.getResponse_code(),
					alog.getContent_length(), alog.getRequest_time());
			logger.info(logalys.toString());
			collector.emit("alogs", new Values(tx, ukey, logalys));
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getStackTrace().toString());
		}
		
	}
	
	@Override
	public void cleanup() {
		System.out.println("shuffTransBolt cleanup");
	}
	
}
