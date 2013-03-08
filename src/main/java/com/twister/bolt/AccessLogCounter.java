package com.twister.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.twister.nio.log.AccessLog;
import com.twister.nio.log.AccessLogStatics;

public class AccessLogCounter extends BaseRichBolt {
	private static final Logger LOGR = LoggerFactory.getLogger(WordCountBolt.class);
	private static final long serialVersionUID = 2246728833921545677L;
	Integer taskid;
	String name;
	
	OutputCollector collector;
	Map<String, Integer> icountMap;
	AccessLogStatics alogstat;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.taskid = context.getThisTaskId();
		this.icountMap = new HashMap<String, Integer>();
		alogstat = new AccessLogStatics();
		LOGR.info(String.format(" AccessLogCounter componentId name :%s,task id :%s ", this.name, this.taskid));
	}
	
	@Override
	public void execute(Tuple input) {
		// 提取单词出现次数
		int count = 0;
		for (int i = 0; i < input.size(); i++) {
			AccessLog alog = (AccessLog) input.getValue(i);
			String ikey = alog.jiekouKey();
			System.out.println(input.size());
			LOGR.debug(alog.toString());
			if (icountMap.containsKey(ikey)) {
				count = icountMap.get(ikey).intValue();
			}
			// 更新单词出现次数
			count += 1;
			icountMap.put(ikey, count);
		}
		
		ArrayList<String> keys = new ArrayList<String>(icountMap.keySet());
		Collections.sort(keys);
		for (String ikey : keys) {
			// 发射统计结果
			collector.emit(new Values(ikey, icountMap.get(ikey)));
			LOGR.info(String.format("AccessLogCounter execute result is:%s : %s ", ikey, icountMap.get(ikey)));
		}
		
		// send ok
		collector.ack(input);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ikey", "cnt"));
	}
	
	@Override
	public void cleanup() {
		
	}
	
}
