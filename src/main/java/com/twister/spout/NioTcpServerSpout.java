package com.twister.spout;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
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
	private ServerBootstrap bootstrap;
	private ChannelFactory channelFactory;
	private Channel serverChannel;
	private final static boolean isdebug = Constants.isdebug;
	private volatile boolean running = false;
	private MongoManager mgo;
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

	public boolean isRunning() {
		return running;
	}

	public boolean isdebug() {
		return isdebug;
	}

	@Override
	public void open(Map cfg, TopologyContext tc, SpoutOutputCollector collector) {
		this.collector = collector;
		this.conf = cfg;
		this.context = tc;
		this.componentId = context.getThisComponentId();
		this.taskid = context.getThisTaskId();
		this.tips = String.format("componentId name :%s,task id :%s ", this.componentId, this.taskid);
		logger.info(tips + conf.size() + context.getStormId());
		mgo = MongoManager.getInstance();
		channelFactory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		bootstrap = new ServerBootstrap(channelFactory);
		try {
			// Set up the pipeline factory.
			bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
				@Override
				public ChannelPipeline getPipeline() throws Exception {
					ChannelPipeline pipeline = Channels.pipeline();
					// Add the text line codec combination first,
					pipeline.addLast("framer", new LineBasedFrameDecoder(Constants.MaxFrameLength));
					pipeline.addLast("decoder", new StringDecoder());
					pipeline.addLast("encoder", new StringEncoder());
					// and then business logic.
					pipeline.addLast("handler", new TcpEventHandler());
					return pipeline;
				}
			});
			// 这里设置tcpNoDelay和keepAlive参数，前面的child前缀必须要加上，用来指明这个参数将被应用到接收到的Channels
			bootstrap.setOption("reuseAddress", true);
			bootstrap.setOption("child.tcpNoDelay", true);
			bootstrap.setOption("child.keepAlive", true);
			// Bind and start to accept incoming connections.
			serverChannel = bootstrap.bind(new InetSocketAddress(InetAddress.getLocalHost(), port));
			running = true;
			localip = InetAddress.getLocalHost().getHostAddress();
			String dts = Common.createDataStr();
			String serinfo = "tcp:" + localip + ":" + port + " " + dts;
			BasicDBObject sermap = new BasicDBObject();
			sermap.put("ip", localip);
			sermap.put("port", port);
			sermap.put("kind", "tcp");
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

	public class TcpEventHandler extends SimpleChannelUpstreamHandler {
		public TcpEventHandler() {
		}

		@Override
		public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
			if (e instanceof ChannelStateEvent) {
				// ChannelStateEvent evt = (ChannelStateEvent) e;
				// System.out.println(evt.getState());
			}
			// Let SimpleChannelHandler call actual event handler methods below.
			super.handleUpstream(ctx, e);
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
			// transLines.incrementAndGet();
			try {
				// see LineBasedFrameDecoder
				String buffer = (String) e.getMessage();
				// SynchronousQueue put ,spout poll
				// logger.debug("recvd length " + buffer.length() + "/" + transLines + " bytes [" + buffer.toString() + "]");
				synchronized (this) {
					queue.offer(buffer);
				}
			} catch (Exception e2) {
				logger.error(e2.getStackTrace().toString());
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
