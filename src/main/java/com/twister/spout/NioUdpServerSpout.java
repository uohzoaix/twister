package com.twister.spout;

import java.net.InetAddress;

import java.util.ArrayList;

import java.util.List;
import java.util.Map;
import java.util.Queue;

import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Queues;
import com.twister.entity.AccessLog;
import com.twister.nio.server.NioUdpServer;

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
public class NioUdpServerSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 2549996244567293L;
	private final Logger logger = LoggerFactory.getLogger(NioUdpServerSpout.class.getName());
	public SpoutOutputCollector collector;
	public Map<String, Object> conf;
	public TopologyContext context;
	public String componentId;
	public int taskid;
	private String tips = "";
	private String localip = "127.0.0.1";
	public final Pattern Ipv4 = Common.Ipv4;
	private volatile boolean running = false;
	private Fields _fields = new Fields("AccessLog");
	private String[] ports;
	private int port = 10237;
	private final List<NioUdpServer> ser = new ArrayList<NioUdpServer>();
	private final Queue<String> queue = Queues.newConcurrentLinkedQueue();
	
	public NioUdpServerSpout(int port) {
		this.port = port;
	}
	
	public boolean isRunning() {
		return running;
	}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.conf = conf;
		this.context = context;
		this.componentId = context.getThisComponentId();
		this.taskid = context.getThisTaskId();
		this.tips = String.format("componentId name :%s,task id :%s ", this.componentId, this.taskid);
		try {
			localip = InetAddress.getLocalHost().getHostAddress();
			String sk = localip + ":" + port;
			NioUdpServer ts = new NioUdpServer(queue, port, false);
			ts.run();
			ser.add(ts);
			logger.info(tips + "" + localip + ":" + port);
		} catch (Exception e) {
			NioUdpServer ts2 = new NioUdpServer(queue, port + 4, false);
			ts2.run();
			ser.add(ts2);
			logger.info(tips + "random " + localip + ":" + port);
			logger.error(e.getStackTrace().toString());
		}
		
	}
	
	/**
	 * 消费者
	 */
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
						// spoutLines++;
						alog = new AccessLog(line);
						if (alog != null) {
							// logger.debug(spoutLines + " " + line);
							collector.emit(new Values(alog));
							logger.info(alog.toString());
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
			for (NioUdpServer ts : ser) {
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
