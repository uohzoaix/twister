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
	public static int GLOB = 0;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	/**
	 * tuple 接收一行日志，或者一个accesslog对象
	 */
	@Override
	public void execute(Tuple input) {
		AccessLog alog = null;
		String line = "";
		try {
			for (int i = 0; i < input.size(); i++) {
				Object obj = input.getValue(i);
				GLOB++;
				if (obj instanceof AccessLog) {
					alog = (AccessLog) obj;
				} else if (obj instanceof String) {
					line = (String) obj;
					if (line.endsWith("\n")) {
						line = line.substring(0, line.indexOf("\n"));
					}
					alog = new AccessLog(line);
				} else {
					continue;
				}
				// ukey=time#rely#server#uriname
				// 20120613#10:01:00#0#/home
				if (alog != null && alog.outKey().length() > 20) {
					// LOGR.info(alog.toString());
					// 转化成少的pojo由code算出cnt_error等,不累加直接发过去
					AccessLogAnalysis logalys = new AccessLogAnalysis(alog.outKey(), alog.getResponse_code(),
							alog.getContent_length(), alog.getRequest_time());
					// LOGR.info(logalys.toString());
					
					collector.emit(new Values(alog.outKey(), logalys));
					if (alog.outKey().contains("initial")) {
						
					}
				} else {
					LOGR.info("format error" + line);
				}
			}
			LOGR.info("shuff=====initial=====" + GLOB);
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
		declarer.declare(new Fields("ukey", "AccessLogAnalysis"));
	}
	
}
