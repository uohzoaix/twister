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
import com.twister.nio.log.AccessLogAnalysis;

/**
 * 将分析结果随机分发到Bolt的任务，保证每个任务获得相等数量的tuple
 * 
 * @author guoqing
 * 
 */
public class AccessLogShuffle extends BaseRichBolt {
	
	private static final long serialVersionUID = 1896733498701080791L;
	public static Logger LOGR = LoggerFactory.getLogger(AccessLogShuffle.class);
	OutputCollector collector;
	 
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void execute(Tuple input) {
		try {
			for (int i = 0; i < input.size(); i++) {
				AccessLog alog = (AccessLog) input.getValue(i);
				System.out.println(input.size());
				LOGR.debug(alog.toString());
				//转化成少的pojo由code算出cnt_error等,不累加直接发过去
				AccessLogAnalysis logalys=new AccessLogAnalysis(alog.outKey(),alog.getResponse_code(),alog.getContent_length(),alog.getRequest_time());
				LOGR.debug(logalys.toString());
				collector.emit(new Values(alog.outKey(),logalys));
			}
			// 通过ack操作确认这个tuple被成功处理
			collector.ack(input);
		} catch (Exception e) {
			e.printStackTrace();
			LOGR.error(e.getStackTrace().toString());
		}
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// out object=AccessLogAnalysis,fieldname=AccessLog
		declarer.declare(new Fields("ukey","AccessLogAnalysis"));
	}
	
}
