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

import com.twister.entity.AccessLog;
import com.twister.entity.AccessLogAnalysis;

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
	private Long GLOB = 0l;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	/**
	 * tuple 接收一行日志，或者一个accesslog对象
	 */
	@Override
	public void execute(Tuple input) {
		// Object alg = input.getValueByField("AccessLog");
		try {
			
			Object obj = input.getValueByField("AccessLog");
			this.emitAccessLogAnalysis(collector, obj);
			// LOGR.info("shuff=====initial=====" + GLOB);
			// 通过ack操作确认这个tuple被成功处理
			collector.ack(input);
		} catch (Exception e) {
			e.printStackTrace();
			LOGR.info(e.getStackTrace().toString());
		}
		
	}
	
	public void emitAccessLogAnalysis(OutputCollector collector, Object obj) {
		AccessLog alog = null;
		if (obj instanceof AccessLog) {
			alog = (AccessLog) obj;
			// ukey=time#rely#server#uriname
			// 20120613#10:01:00#0#/home
			if (alog != null && alog.outKey().length() > 20) {
				// LOGR.info(alog.toString());
				// 转化成少的pojo由code算出cnt_error等,不累加直接发过去
				AccessLogAnalysis logalys = new AccessLogAnalysis(alog.outKey(), alog.getResponse_code(),
						alog.getContent_length(), alog.getRequest_time());
				// LOGR.info(logalys.toString());
				// GLOB++;
				collector.emit(new Values(alog.outKey(), logalys));
			}
			
		} else if (obj instanceof String) {
			String txt = (String) obj;
			String[] lines = txt.split("\n");
			for (int i = 0; i < lines.length; i++) {
				String line = lines[i];
				alog = new AccessLog(line);
				// ukey=ver#time#rely#server
				// 0#20120613#10:01:00#0
				if (alog != null && alog.outKey().length() > 20) {
					// LOGR.info(alog.toString());
					// 转化成少的pojo由code算出cnt_error等,不累加直接发过去
					AccessLogAnalysis logalys = new AccessLogAnalysis(alog.outKey(), alog.getResponse_code(),
							alog.getContent_length(), alog.getRequest_time());
					// GLOB++;
					collector.emit(new Values(alog.outKey(), logalys));
				}
			}
			
		} else {
			// System.out.println("input error");
		}
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// out object=AccessLogAnalysis,fieldname=AccessLog
		declarer.declare(new Fields("ukey", "AccessLogAnalysis"));
	}
	
}
