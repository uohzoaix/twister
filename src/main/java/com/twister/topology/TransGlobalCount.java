package com.twister.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.topology.base.BaseTransactionalSpout;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.Serializable;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import java.util.HashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import com.google.common.collect.Queues;
import com.twister.entity.AccessLog;

import com.twister.storage.AccessLogCacheManager;
import com.twister.storage.cache.EhcacheMap;
import com.twister.storage.redis.JedisManager;
import com.twister.storage.redis.JedisManager.JedisExpireHelps;
import com.twister.utils.AppsConfig;
import com.twister.utils.Common;
import com.twister.utils.FileUtils;

/**
 * This is a basic example of a transactional topology. It keeps a count of the
 * number of tuples seen so far in a database. The source of data and the
 * databases are mocked out as in memory maps for demonstration purposes. This
 * class is defined in depth on the wiki at
 * https://github.com/nathanmarz/storm/wiki/Transactional-topologies
 */
public class TransGlobalCount {
	public static Logger logger = LoggerFactory.getLogger(TransGlobalCount.class);
	public static final int MAX_TRANSACTION_SIZE = 30;
	public static int PART_NUM = 4;
	
	public static class TransMeta implements Serializable {
		long index;
		long amt;
		
		public TransMeta() {
			
		}
		
		public TransMeta(long index, long amt) {
			this.index = index;
			this.amt = amt;
		}
		
		@Override
		public String toString() {
			return "index: " + index + "; amt: " + amt;
		}
	}
	
	public static class RegPartitionedState {
		public static EhcacheMap<String, Object> _states;
		public static final String NEXT_READ = "NEXT_READ";
		public static final String NEXT_WRITE = "NEXT_WRITE";
		public static final String PARTIIIONE = "PARTIIIONE";
		
		public RegPartitionedState() {
			_states = new EhcacheMap<String, Object>("defaultMap");
		}
		
		public long getAvailableToRead(int partition, long current) {
			return getNextWrite(partition) - current;
		}
		
		public long getNextRead(int partition) {
			long sNextRead = 1l;
			String nextkey = NEXT_READ + partition;
			if (_states.containsKey(nextkey)) {
				Long.valueOf(_states.get(nextkey).toString());
			}
			return Long.valueOf(sNextRead);
			
		}
		
		public long getNextWrite(int partition) {
			// This key should always exist, in order for the spout to work
			// properly.
			String nextkey = NEXT_WRITE + partition;
			return Long.valueOf(_states.get(nextkey).toString());
		}
		
		public void setNextRead(int partition, long nextRead) {
			String nextkey = NEXT_READ + partition;
			_states.put(nextkey, nextRead);
		}
		
		public List<String> getMessages(int partition, long from, int quantity) {
			String[] keys = new String[quantity];
			List<String> ls = new ArrayList<String>();
			for (int i = 0; i < quantity; i++) {
				keys[i] = PARTIIIONE + partition + ":" + (i + from);
				String v = (String) _states.get(keys[i]);
				ls.add(v);
			}
			return ls;
		}
		
		public void addMessage(int partition, String line) {
			String nextk = NEXT_WRITE + partition;
			long nextv = Long.valueOf(_states.get(nextk).toString());
			String nextkk = PARTIIIONE + partition + ":" + nextv;
			_states.put(PARTIIIONE + nextkk, line);
			nextv += 1;
			_states.put(NEXT_WRITE + partition, nextv);
		}
		
	}
	
	public static class TransSpout implements IOpaquePartitionedTransactionalSpout<TransMeta> {
		private long _id = 0l;
		private static int port = 10236;
		private int _takeAmt;
		
		private Fields _outFields = new Fields("txid", "amt", "word");
		
		public TransSpout() {
		}
		
		public static class LogCoordinator implements IOpaquePartitionedTransactionalSpout.Coordinator {
			
			@Override
			public boolean isReady() {
				return true;
			}
			
			@Override
			public void close() {
			}
			
		}
		
		public static class LogEmitter implements IOpaquePartitionedTransactionalSpout.Emitter<TransMeta> {
			public Map<String, Object> conf;
			public TopologyContext context;
			public String componentId;
			public int taskid;
			
			private ServerBootstrap bootstrap;
			private ChannelFactory channelFactory;
			private Channel serverChannel;
			
			private final static int bufferSize = 1024;
			private volatile boolean running = false;
			private AccessLogCacheManager alogManager; // reids
			
			// SynchronousQueue or ArrayBlockingQueue ,LinkedList;
			private static Queue<String> queue = Queues.newConcurrentLinkedQueue();
			private String localip = "127.0.0.1";
			private RegPartitionedState rq = new RegPartitionedState();
			
			public LogEmitter(Map conf, TopologyContext context) {
				
				this.conf = conf;
				this.context = context;
				this.componentId = context.getThisComponentId();
				this.taskid = context.getThisTaskId();
				alogManager = new AccessLogCacheManager();
				Jedis jedis = JedisManager.getInstance().getMasterJedis();
				channelFactory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool());
				bootstrap = new ServerBootstrap(channelFactory);
				try {
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
							pipeline.addLast("handler", new TcpEventHandler(queue));
							return pipeline;
						}
					});
					// 这里设置tcpNoDelay和keepAlive参数，前面的child前缀必须要加上，用来指明这个参数将被应用到接收到的Channels
					bootstrap.setOption("reuseAddress", true);
					bootstrap.setOption("child.tcpNoDelay", true);
					bootstrap.setOption("child.keepAlive", true);
					
					// Bind and start to accept incoming connections.
					serverChannel = bootstrap.bind(new InetSocketAddress(InetAddress.getLocalHost(), port));
					localip = InetAddress.getLocalHost().getHostAddress();
					running = true;
					
					String dts = Common.createDataTimeStr();
					String serinfo = "TcpSpout:" + localip + ":" + port;
					jedis.select(JedisExpireHelps.DBIndex);
					jedis.set(serinfo, dts);
					jedis.expire(serinfo, JedisExpireHelps.expire_2DAY);
					// save ip:port to tmpfile
					String tmpfile = AppsConfig.getInstance().getValue("save.spoutIpPort.file");
					FileUtils.writeFile(tmpfile, serinfo, true);
					// logger.debug(progName +
					// " tcp spout started,listening on " +
					// localip + ":" + port);
				} catch (UnknownHostException e) {
					logger.error(e.getStackTrace().toString());
				} catch (Exception e) {
					logger.error(e.getStackTrace().toString());
				}
				
			}
			
			@Override
			public TransMeta emitPartitionBatch(TransactionAttempt tx, BatchOutputCollector collector, int partition,
					TransMeta lastPartitionMeta) {
				long nextRead;
				
				if (lastPartitionMeta == null)
					nextRead = rq.getNextRead(partition);
				else {
					nextRead = lastPartitionMeta.index + lastPartitionMeta.amt;
					rq.setNextRead(partition, nextRead); // Move the cursor
				}
				
				long atid = rq.getAvailableToRead(partition, nextRead);
				atid = atid > MAX_TRANSACTION_SIZE ? MAX_TRANSACTION_SIZE : atid;
				TransMeta metadata = new TransMeta(nextRead, (int) atid);
				emitMessages(tx, collector, partition, metadata);
				return metadata;
				
			}
			
			private void emitMessages(TransactionAttempt tx, BatchOutputCollector collector, int partition,
					TransMeta partitionMeta) {
				if (partitionMeta.amt <= 0)
					return;
				
				List<String> messages = rq.getMessages(partition, partitionMeta.index, (int) partitionMeta.amt);
				long tid = partitionMeta.index;
				for (String txt : messages) {
					AccessLog alog = null;
					try {
						if (txt != null && txt.length() > 10) {
							// send obj
							String[] lines = txt.split("\n");
							for (int i = 0; i < lines.length; i++) {
								String line = lines[i];
								if (Common.Ipv4.matcher(line).find()) {
									alog = new AccessLog(line);
									if (alog != null) {
										logger.debug(" " + line);
										// collector.emit(new Values(alog));
										collector.emit(new Values(tx, "" + tid, alog.getUri_name()));
										logger.info("++++++++++++++++++ " + alog.toString());
									}
								}
								
							}
						}
						
					} catch (Exception e) {
						logger.error(e.getStackTrace().toString());
					}
					tid += 1;
				}
			}
			
			@Override
			public void close() {
				System.out.println("stopping UDP server");
				serverChannel.close();
				channelFactory.releaseExternalResources();
				bootstrap.releaseExternalResources();
				running = false;
				System.out.println("server stopped");
				
			}
			
			@Override
			public int numPartitions() {
				return PART_NUM;
			}
			
		}
		
		@Override
		public backtype.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout.Emitter<TransMeta> getEmitter(
				Map conf, TopologyContext context) {
			return new LogEmitter(conf, context);
		}
		
		@Override
		public backtype.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout.Coordinator getCoordinator(
				Map conf, TopologyContext context) {
			return new LogCoordinator();
		}
		
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(_outFields);
		}
		
		@Override
		public Map<String, Object> getComponentConfiguration() {
			return null;
		}
		
		public static class TcpEventHandler extends SimpleChannelUpstreamHandler {
			private Queue<String> queue;
			private long transLines = 0;
			private RegPartitionedState rq = new RegPartitionedState();
			
			public TcpEventHandler(final Queue<String> queue) {
				this.queue = queue;
			}
			
			@Override
			public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
				try {
					// see LineBasedFrameDecoder
					String buffer = (String) e.getMessage();
					transLines += 1;
					// SynchronousQueue put ,spout poll
					// this.queue.offer(buffer);
					int pt = (int) transLines % PART_NUM;
					rq.addMessage(pt, buffer);
					logger.info("partnum " + pt + "recvd length " + buffer.length() + "/" + transLines + " bytes ["
							+ buffer.toString() + "]");
					
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
	
	public static class Value {
		BigInteger txid;
		String ukey = "";
		int count = 0;
		
		public void addval() {
			count += 1;
		}
		
	}
	
	public static Map<String, Value> DATABASE = new HashMap<String, Value>();
	public static final String GLOBAL_COUNT_KEY = "GLOBAL-COUNT";
	
	public static class BatchCount extends BaseBatchBolt {
		Object _id;
		BatchOutputCollector _collector;
		HashMap<String, Value> hsword;
		int _count = 0;
		
		@Override
		public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
			_collector = collector;
			_id = id;
			hsword = new HashMap<String, Value>();
		}
		
		@Override
		public void execute(Tuple tuple) {
			String txid = tuple.getValueByField("txid").toString();
			String amt = tuple.getValueByField("amt").toString();
			String ukey = tuple.getValueByField("word").toString();
			Value newval = new Value();
			if (hsword.containsKey(ukey)) {
				newval.ukey = ukey;
				newval.txid = (BigInteger) this._id;
				newval.count = newval.count + 1;
				hsword.put(ukey, newval);
			} else {
				newval.ukey = ukey;
				newval.txid = (BigInteger) this._id;
				newval.count = 1;
				hsword.put(ukey, newval);
			}
			_count++;
		}
		
		@Override
		public void finishBatch() {
			for (String tag : hsword.keySet()) {
				Value newval = hsword.get(tag);
				_collector.emit(new Values(this._id, newval.ukey, newval.count));
			}
		}
		
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "ukey", "count"));
		}
	}
	
	public static class UpdateGlobalCount extends BaseTransactionalBolt implements ICommitter {
		TransactionAttempt _attempt;
		BatchOutputCollector _collector;
		
		int _sum = 0;
		String ukey = "";
		
		@Override
		public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector,
				TransactionAttempt attempt) {
			_collector = collector;
			_attempt = attempt;
		}
		
		@Override
		public void execute(Tuple tuple) {
			ukey = tuple.getString(1);
			_sum += tuple.getInteger(2);
		}
		
		@Override
		public void finishBatch() {
			Value val = DATABASE.get(ukey);
			Value newval;
			if (val == null || !val.txid.equals(_attempt.getTransactionId())) {
				newval = new Value();
				newval.txid = _attempt.getTransactionId();
				newval.ukey = ukey;
				if (val == null) {
					newval.count = _sum;
				} else {
					newval.count = _sum + val.count;
				}
				DATABASE.put(ukey, newval);
			} else {
				newval = val;
			}
			logger.info("_attempt: " + _attempt + " ukey: " + newval.ukey + " count: " + newval.count);
			_collector.emit(new Values(_attempt, newval.ukey, newval.count));
		}
		
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "ukey", "sum"));
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		TransSpout spout = new TransSpout();
		TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("global-count", "spout", spout, 3);
		
		builder.setBolt("partial-count", new BatchCount(), 5)
				.fieldsGrouping("spout", new Fields("id", "ukey", "count"));
		builder.setBolt("sum", new UpdateGlobalCount()).globalGrouping("partial-count");
		
		LocalCluster cluster = new LocalCluster();
		
		Config config = new Config();
		config.setDebug(true);
		config.setMaxSpoutPending(3);
		
		cluster.submitTopology("global-count-topology", config, builder.buildTopology());
		System.out.println("==========================");
		Thread.sleep(3000);
		cluster.shutdown();
	}
}