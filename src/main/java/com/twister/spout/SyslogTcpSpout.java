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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;


/**
 * Spout to feed messages into Storm from an TCP Socket.
 * <p>
 * This spout emits tuples containing only one field, named "packet" for each packet receive on the TCP socket.
 * </p>
 */
public class SyslogTcpSpout extends BaseRichSpout {

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

    public SyslogTcpSpout() {
        this.port = DEFAULT_SYSLOG_TCP_PORT;
    }

    public SyslogTcpSpout(int port) {
        this.port = port;
    }


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        Preconditions.checkState(server == null, "SyslogTcpSpout already open on port " + port );
        this.collector = collector;
        try {
            server = new ServerSocket(port);
            logger.info("Opening SyslogTcpSpout on port " + port);
            Socket connection = server.accept();
            reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Override
    public void close() {
//        if (!server.isClosed()) {
            try {
                server.close();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            logger.info("Closing SyslogTcpSpout on port " + port);
//        }
    }

    @Override
    public void nextTuple() {
        while (true) {
            try {
                String packet = reader.readLine();
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