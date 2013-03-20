package com.twister.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * Spout to feed messages into Storm from an UDP Socket.
 * <p>
 * This spout emits tuples containing only one field, named "packet" for each packet receive on the UDP socket.
 * TODO Point to point
 * </p>
 * <pre></pre>
 * 
 * @author guoqing  
 * 
 */
public class SyslogUdpSpout extends BaseRichSpout {

    /**
	 * 
	 */
	private static final long serialVersionUID = 2549996244317249537L;

	protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final static int DEFAULT_SYSLOG_UDP_PORT = 1234;
    // This limit stems from the maximum supported UDP size of 65535 octets specified in RFC 768
    private final static int MAX_SESSAGE_SIZE = 65535;

    private int port;
    private SpoutOutputCollector collector;
    private DatagramSocket socket;
    private InetAddress ip;
    private Fields _fields=new Fields("AccessLog");

    public SyslogUdpSpout() {
        this.port = DEFAULT_SYSLOG_UDP_PORT;
        try {
			this.ip = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {			 
			e.printStackTrace();
		}
    }
    
    public SyslogUdpSpout(int port) {
        this.port = port;
        try {
			this.ip = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {			 
			e.printStackTrace();
		}
    }
    
    public SyslogUdpSpout(int port,InetAddress ip) {
        this.port = port;
        try {
			this.ip =ip;
		} catch (Exception e) {			 
			e.printStackTrace();
		}
    }


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
        	// ucp accept, bound ip:port 创建接收方的套接字,并制定端口号和IP地址
            this.socket = new DatagramSocket(port,ip);
            Preconditions.checkState(socket.isBound(), "Socket on port "+ port +" already bound.");
            logger.info("Opening SyslogUdpSpout on port " + port+" ip:"+ip);
        } catch (SocketException e) {
            throw new RuntimeException(e);
            // TODO
        }
    }

    @Override
    public void close() {
        if (!socket.isClosed()) {
            socket.close();
            logger.info("Closing SyslogUdpSpout on port " + port);
        }
    }

    @Override
    public void nextTuple() {
        byte[] buffer = new byte[MAX_SESSAGE_SIZE];
        DatagramPacket dp = new DatagramPacket(buffer, MAX_SESSAGE_SIZE);
        while (true) {
            try {
            	//接收syslog-udp的套接字
                socket.receive(dp);
                String packet = new String(dp.getData(), 0, dp.getLength());
                if (packet == null) continue;
                collector.emit(new Values(packet));
                return;
            } catch (IOException e) {
                // TODO
                throw new RuntimeException(e);
            }
        }
    }

    public boolean isDistributed() {
        return false;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_fields);
    }

    @Override
    public void ack(Object o) {
    }

    @Override
    public void fail(Object o) {
        // TODO log ?
    }
}