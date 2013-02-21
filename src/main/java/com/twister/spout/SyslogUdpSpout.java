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
import java.net.SocketException;
import java.util.Map;

/**
 * Spout to feed messages into Storm from an UDP Socket.
 * <p>
 * This spout emits tuples containing only one field, named "packet" for each packet receive on the UDP socket.
 * TODO Point to point
 * </p>
 */
public class SyslogUdpSpout extends BaseRichSpout {

    /**
	 * 
	 */
	private static final long serialVersionUID = 2549996244317249537L;

	protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final static int DEFAULT_SYSLOG_UDP_PORT = 514;
    // This limit stems from the maximum supported UDP size of 65535 octets specified in RFC 768
    private final static int MAX_SESSAGE_SIZE = 65535;

    private int port;
    private SpoutOutputCollector collector;
    private DatagramSocket socket;


    public SyslogUdpSpout() {
        this.port = DEFAULT_SYSLOG_UDP_PORT;
    }

    public SyslogUdpSpout(int port) {
        this.port = port;
    }


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            this.socket = new DatagramSocket(port);
            Preconditions.checkState(socket.isBound(), "Socket on port "+ port +" already bound.");
            logger.info("Opening SyslogUdpSpout on port " + port);
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