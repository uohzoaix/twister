package com.twister.spout;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.IRichSpout;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.twister.TupleEmitter;
import com.twister.utils.SocketUtils;
import com.twister.utils.TupleUtils;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SyslogTcpSpoutTest {

    private IRichSpout spout;

    @Mock
    private SpoutOutputCollector collector;

    @Test
    public void syslogTcp() throws Exception {
        // Given
        int count = 10;
        int port = 10678; // Creates an unbound server socket.
        spout = new SyslogNioTcpSpout(port);
        //spout = new SyslogTcpSpout(port);

        CountDownLatch done = new CountDownLatch(1);
        TupleEmitter emitter = new TupleEmitter(spout, collector, done, count); // Call next tuple
        emitter.start(); // Start the thread who calls nextTuple

        // When
        Socket socket = new Socket("vm01.pc.zgq",port);
        List<String> packets = SocketUtils.writeRandomPackets(socket, count);
        
        // Then
        assertTrue(done.await(5, SECONDS));
        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(collector, times(count)).emit(captor.capture());
        List<String> values = TupleUtils.unwrap(captor.getAllValues(), 0);
            assertEquals(packets, values);
    }
}
