package com.twister.spout;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import java.util.Map;
import java.util.Queue;

import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
import org.jboss.netty.handler.codec.frame.LineBasedFrameDecoder;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Queues;
import com.mongodb.BasicDBObject;
import com.twister.entity.AccessLog;
import com.twister.storage.mongo.MongoManager;
import com.twister.utils.Common;
import com.twister.utils.Constants;
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
	private final static boolean isdebug = Constants.isdebug;
	private volatile boolean running = false;
	private Fields _fields = new Fields("AccessLog");
	private int port = 10237;
	private ConnectionlessBootstrap bootstrap;
	private ChannelFactory channelFactory;
	private Channel serverChannel;
	private final static int bufferSize = 1024;
	private long transLines = 0l;

	// SynchronousQueue or ArrayBlockingQueue ,LinkedList;
	private final Queue<String> queue = Queues.newConcurrentLinkedQueue();
	private MongoManager mgo;

	public NioUdpServerSpout(int port) {
		this.port = port;
	}

	public boolean isRunning() {
		return running;
	}

	public boolean isdebug() {
		return isdebug;
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
			mgo = MongoManager.getInstance();
			channelFactory = new NioDatagramChannelFactory(Executors.newCachedThreadPool(), 4);
			bootstrap = new ConnectionlessBootstrap(channelFactory);
			// Set up the pipeline factory.
			bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
				@Override
				public ChannelPipeline getPipeline() throws Exception {
					ChannelPipeline pipeline = Channels.pipeline();
					// Add the text line codec combination first,
					pipeline.addLast("framer", new LineBasedFrameDecoder(bufferSize));
					pipeline.addLast("decoder", new StringDecoder());
					pipeline.addLast("encoder", new StringEncoder());
					// and then business logic.
					pipeline.addLast("handler", new UdpEventHandler());
					return pipeline;
				}
			});
			bootstrap.setOption("reuseAddress", true);
			// bootstrap.setOption("child.tcpNoDelay", true);
			bootstrap.setOption("child.keepAlive", true);
			serverChannel = bootstrap.bind(new InetSocketAddress(InetAddress.getLocalHost(), port));
			String localip = InetAddress.getLocalHost().getHostAddress();
			running = true;
			String dts = Common.createDataStr();
			String serinfo = "udp:" + localip + ":" + port + " " + dts;
			BasicDBObject sermap = new BasicDBObject();
			sermap.put("ip", localip);
			sermap.put("port", port);
			sermap.put("kind", "udp");
			sermap.put("desc", "spout");
			sermap.put("day", dts);
			mgo.insertOrUpdate(Constants.SpoutTable, sermap, sermap);
			logger.info("服务端已准备好 " + serinfo);
			logger.info(tips + "" + localip + ":" + port);
		} catch (UnknownHostException e) {
			logger.error(e.getStackTrace().toString());
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
			logger.info("stopping UDP server");
			channelFactory.releaseExternalResources();
			serverChannel.close();
			bootstrap.releaseExternalResources();
			running = false;
			logger.info("server stopped");
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

	/**
	 * 生产者
	 */
	public class UdpEventHandler extends SimpleChannelUpstreamHandler {
		public UdpEventHandler() {
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
			try {
				// see LineBasedFrameDecoder
				String buffer = (String) e.getMessage();
				// transLines += 1;
				// logger.info("udp recvd length " + buffer.length() + "/" + transLines + " bytes [" + buffer.toString() + "] " + isdebug);
				synchronized (this) {
					queue.offer(buffer);
				}
			} catch (Exception e2) {
				logger.error(transLines + " " + e2.getStackTrace().toString());
			}
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
			// Close the connection when an exception is raised.
			logger.warn("Unexpected exception from downstream.", e.getCause());
			// e.getChannel().close();
		}
	}
}
