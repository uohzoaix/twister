package com.twister.bolt;

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

/**
 * 随机分发tuple到Bolt的任务，保证每个任务获得相等数量的tuple
 * 
 * @author guoqing
 * 
 */
public class AccessLogShuffle extends BaseRichBolt {
	
	private static final long serialVersionUID = 1896733498701080791L;
	public static Logger LOGR = LoggerFactory.getLogger(WordExtractorBolt.class);
	OutputCollector collector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void execute(Tuple input) {
		for (int i = 0; i < input.size(); i++) {
			AccessLog alog = (AccessLog) input.getValue(i);
			System.out.println(input.size());
			LOGR.debug(alog.toString());
			collector.emit(new Values(alog));
		}
		// 通过ack操作确认这个tuple被成功处理
		collector.ack(input);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// out object
		declarer.declare(new Fields("AccessLog"));
	}
	
}
