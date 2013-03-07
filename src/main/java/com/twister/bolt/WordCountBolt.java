package com.twister.bolt;

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

/**
 * @author guoqing
 * 
 */
public class WordCountBolt extends BaseRichBolt {
	private static final Logger LOGR = LoggerFactory.getLogger(WordCountBolt.class);
	private static final long serialVersionUID = 2246728833921545675L;
	Integer taskid;
	String name;
	
	OutputCollector collector;
	Map<String, Integer> wordCountMap;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.taskid = context.getThisTaskId();
		this.wordCountMap = new HashMap<String, Integer>();
		LOGR.info(String.format(" WordCountBolt componentId name :%s,task id :%s ", this.name, this.taskid));
	}
	
	@Override
	public void execute(Tuple input) {
		// 提取单词出现次数
		String word = input.getString(0);
		
		LOGR.info(word);
		int count = 0;
		if (wordCountMap.containsKey(word)) {
			count = wordCountMap.get(word).intValue();
		}
		// 更新单词出现次数
		count += 1;
		wordCountMap.put(word, count);
		
		// 发射统计结果
		collector.emit(new Values(word, wordCountMap.get(word)));
		LOGR.info(String.format("WordCountBolt execute result is:%s : %s ", word, wordCountMap.get(word)));
		// send ok
		collector.ack(input);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}
	
	@Override
	public void cleanup() {
		
	}
	
}
