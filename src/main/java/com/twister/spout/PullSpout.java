package com.twister.spout;

import java.util.Map;

import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twister.entity.AccessLog;

import com.twister.jzmq.PullCli;

import com.twister.storage.mongo.MongoManager;
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
public class PullSpout extends BaseRichSpout {

	private static final long serialVersionUID = 2549996244567293L;
	private final Logger logger = LoggerFactory.getLogger(PullSpout.class.getName());
	public SpoutOutputCollector collector;
	public Map<String, Object> conf;
	public TopologyContext context;
	public String componentId;
	public int taskid;
	private String tips = "";
	private String localip = "127.0.0.1";
	public final Pattern Ipv4 = Common.Ipv4;
	private Fields _fields = new Fields("AccessLog");
	private int port = 10237;
	@SuppressWarnings("unused")
	private MongoManager mgo;
	private PullCli pull;

	public PullSpout(String host, int port) {
		this.localip = host;
		this.port = port;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.conf = conf;
		this.context = context;
		this.componentId = context.getThisComponentId();
		this.taskid = context.getThisTaskId();
		this.tips = String.format("componentId name :%s,task id :%s ", this.componentId, this.taskid);
		logger.info(tips + conf.size() + context.getStormId());
		mgo = MongoManager.getInstance();
		try {
			pull = new PullCli(localip, port);
			Thread thr1 = new Thread(pull, "pullClient");
			thr1.setDaemon(true);
			thr1.start();
			logger.info(tips + "" + localip + ":" + port);
		} catch (Exception e) {
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
			String txt = pull.recv();
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

		} catch (Exception e) {
			e.printStackTrace();
		}
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
