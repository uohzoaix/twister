package com.twister.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;


/**
 * Spout to feed messages into Storm from an TCP Socket.
 * <p>
 * This spout emits tuples containing only one field, named "packet" for each packet receive on the TCP socket.
 * </p>
 * <pre></pre>
 * 
 * @author guoqing  
 * 
 */
public class SyslogTcpNIOSpout extends BaseRichSpout {

    /**
	 * 
	 */
	private static final long serialVersionUID = -3845095415477875992L;
	protected final Logger logger = LoggerFactory.getLogger(getClass());
    private final static int DEFAULT_SYSLOG_TCP_PORT = 514;

    private SpoutOutputCollector collector;
    private ServerSocket server;
    private BufferedReader reader;
    private int port;
    private InetAddress ip;

    public SyslogTcpNIOSpout() {
        this.port = DEFAULT_SYSLOG_TCP_PORT;
        try {
			this.ip = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {			 
			e.printStackTrace();
		}
    }

    public SyslogTcpNIOSpout(int port) {
        this.port = port;
        try {
			this.ip = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {			 
			e.printStackTrace();
		}
    }
    
    public SyslogTcpNIOSpout(int port,InetAddress ip) {
        this.port = port;
        try {
			this.ip =ip;
		} catch (Exception e) {			 
			e.printStackTrace();
		}
    }



    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        Preconditions.checkState(server == null, "SyslogTcpSpout already open on port " + port );
        this.collector = collector;
        try {
        	//TCPServer accept, 监听端口，准备连接客户端
            server = new ServerSocket(port);            
            this.ip=server.getInetAddress();               
            logger.info("Opening SyslogTcpSpout on port " + port+" ip:"+ip.getHostAddress());  
            
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Override
    public void close() {
 
            try {               
                if (!server.isClosed()) {
                	 server.close();
                }                 
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            logger.info("Closing SyslogTcpSpout on port " + port);
 
    }

	@Override
	public void nextTuple() {
		Socket connection = null;
		try {
			connection = server.accept();
			reader = new BufferedReader(new InputStreamReader(
					connection.getInputStream()));
			while (true) {
				try {
					String packet = reader.readLine();
					logger.info(packet);
					if (packet == null)
						continue;
					collector.emit(new Values(packet));
					return;
				} catch (IOException e) {
					throw new RuntimeException(e);
				}

			}
		} catch (IOException e1) {
			e1.printStackTrace();
		} finally {
			try {
				connection.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

    
    public boolean isDistributed() {
        return false;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("packet"));
    }

    @Override
    public void ack(Object o) {
    }

    @Override
    public void fail(Object o) {
        // TODO log ?
    }
}