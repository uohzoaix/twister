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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.charset.Charset;
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
public class SyslogNioUdpSpout extends BaseRichSpout {

    /**
	 * 
	 */
	private static final long serialVersionUID = 2549996244317249537L;

	protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final static int DEFAULT_SYSLOG_UDP_PORT = 1234;
    // This limit stems from the maximum supported UDP size of 65535 octets specified in RFC 768
    private final static int MAX_SESSAGE_SIZE = 65535;
    private final static int readChunckSize = 1024;
    private int port;
    private SpoutOutputCollector collector;
    private DatagramChannel channel = null;
    private DatagramSocket socket=null;
    private Selector selector = null;
    private InetAddress ip; 
    private ByteBuffer byteBuffer;
    public SyslogNioUdpSpout() {
        this.port = DEFAULT_SYSLOG_UDP_PORT;
        try {
			this.ip = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {			 
			e.printStackTrace();
		}
    }
    
    public SyslogNioUdpSpout(int port) {
        this.port = port;
        try {
			this.ip = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {			 
			e.printStackTrace();
		}
    }
    
    public SyslogNioUdpSpout(int port,InetAddress ip) {
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
		    selector = Selector.open();			 
			// 打开选择器  
			channel = DatagramChannel.open();	// 打开UDP通道  		
			channel.configureBlocking(false);    // 非阻塞 
			channel.socket().setReuseAddress(true);
			socket = channel.socket();
			socket.bind(new InetSocketAddress(port));
			System.out.println("server start!");			 
			channel.register(selector, SelectionKey.OP_READ);  // 向通道注册选择器和对应事件标识,返回对应的SelectionKey			
            Preconditions.checkState(socket.isBound(), "Socket on port "+ port +" already bound.");
            logger.info("Opening SyslogNioUdpSpout on port " + port+" ip:"+ip);
        } catch (IOException e) {
        	e.printStackTrace();
        }
           
    }

    @Override
    public void close() {
        if (!socket.isClosed()) {
        	 try {
        		channel.close();
                socket.close();
                selector.selectNow();				
			} catch (IOException e) {				 
				e.printStackTrace();
			}
			
            logger.info("Closing SyslogUdpSpout on port " + port);
        }
    }

    @Override
    public void nextTuple() {    	
    	byteBuffer = ByteBuffer.allocate(readChunckSize);
        try {
			// 选择一组键，并且相应的通道已经打开
			int lks = selector.select();
			if (lks == 0)
				return;
			Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
			while (iter.hasNext()) {
				SelectionKey sk = (SelectionKey)iter.next();
				iter.remove();
				if (sk.isReadable()) {
					// 在这里datagramChannel与channel实际是同一个对象
					DatagramChannel clientChannel = (DatagramChannel) sk.channel();
					byteBuffer.clear();
					SocketAddress sa = clientChannel.receive(byteBuffer);
					// 将缓冲区准备为数据传出状态
					byteBuffer.flip();				 
					// 测试：通过将收到的ByteBuffer首先通过缺省的编码解码成CharBuffer 再输出
					CharBuffer charBuffer = Charset.defaultCharset().decode(byteBuffer);					
					if (charBuffer.length()>0){
						logger.info("receive message:"+ charBuffer.toString());
						collector.emit(new Values(charBuffer.toString()));
					}
					clientChannel.register(selector, SelectionKey.OP_READ);					 
				}
			}
        } catch (IOException e) {
            // TODO
            throw new RuntimeException(e);
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