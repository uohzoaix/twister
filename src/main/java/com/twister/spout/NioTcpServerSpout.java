package com.twister.spout;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Queues;
import com.twister.entity.AccessLog;
import com.twister.nio.server.NioTcpServer;
import com.twister.utils.Common;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;

import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * is tcp nio server
 * 
 * @author guoqing
 * 
 */
public class NioTcpServerSpout extends BaseRichSpout {	 
	private final Logger logger = LoggerFactory.getLogger(getClass().getName());
	private static final long serialVersionUID = 25499962443175493L;
	private SpoutOutputCollector collector;
	private Map<String, Object> conf;
	private TopologyContext context;
	private String componentId;
	private String tips = "";
	private String localip = "127.0.0.1";
	private int taskid;
	private static final Pattern Ipv4 = Common.Ipv4;
	private final AtomicLong transLines = new AtomicLong();
	private int port = 10236;
	private final List<NioTcpServer> ser = new ArrayList<NioTcpServer>();
	// 共享数据队列
	private final Queue<String> queue = Queues.newConcurrentLinkedQueue();
	private Fields _fields = new Fields("AccessLog");

	/**
	 * 
	 * @param serWorknum
	 */
	public NioTcpServerSpout(int port) {
		this.port = port;
	}

	@Override
	public void open(Map cfg, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.conf = cfg;
		this.context = context;
		this.componentId = context.getThisComponentId();
		this.taskid = context.getThisTaskId();
		this.tips = String.format("componentId name :%s,task id :%s ", this.componentId, this.taskid);
		try {
			localip = InetAddress.getLocalHost().getHostAddress();
			NioTcpServer tcps = new NioTcpServer(queue, port, false);
			tcps.run();
			ser.add(tcps);
			logger.info(tips + "" + localip + ":" + port);
		} catch (Exception e) {
			NioTcpServer tcps2 = new NioTcpServer(queue, port + 4, false);
			tcps2.run();
			ser.add(tcps2);
			logger.info(tips + " random " + localip + ":" + port);
			logger.error(e.getStackTrace().toString());
		}
		
	}
	
	@Override
	public void nextTuple() {
		AccessLog alog = null;
		try {
			String txt = null;
			synchronized (this) {
				txt = queue.poll();
			}
			if (txt != null && txt.length() > 10) {
				// send obj
				String[] lines = txt.split("\n");
				for (int i = 0; i < lines.length; i++) {
					String line = lines[i];
					if (Ipv4.matcher(line).find()) {
						alog = new AccessLog(line);
						if (alog != null) {
							transLines.incrementAndGet();
							// logger.debug(spoutLines + " " + line);
							collector.emit(new Values(alog));
							logger.info(transLines + "  " + alog.toString());
						}
					}
					
				}
			}
			
		} catch (Exception e) {
			logger.error(e.getStackTrace().toString());
		}
		
	}
	
	@Override
	public void close() {
		try {
			for (NioTcpServer ts : ser) {
				ts.stop();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		ser.clear();
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(_fields);
	}
	
	@Override
	public void ack(Object msgid) {
		logger.debug("ack msgid " + msgid.toString());
	}
	
	@Override
	public void fail(Object msgid) {
		logger.debug("fail msgid " + msgid.toString());
	}

}
