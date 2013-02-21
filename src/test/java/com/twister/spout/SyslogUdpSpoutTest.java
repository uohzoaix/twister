package com.twister.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.IRichSpout;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.twister.TupleEmitter;
import com.twister.utils.SocketUtils;
import com.twister.utils.TupleUtils;

import java.net.DatagramSocket;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SyslogUdpSpoutTest {

    private IRichSpout spout;

    @Mock
    private SpoutOutputCollector collector;

    @Test
    public void syslogUdp() throws Exception {
        // Given
        int count = 10;
        int port = 5146;
        spout = new SyslogUdpSpout(port);

        CountDownLatch done = new CountDownLatch(1);
        TupleEmitter emitter = new TupleEmitter(spout, collector, done, count); // Call next tuple
        emitter.start(); // Start the thread who calls nextTuple

        // When
        DatagramSocket socket = new DatagramSocket();
        List<String> packets = SocketUtils.writeRandomPackets(socket, count, port);

        // Then
        assertTrue(done.await(5, SECONDS));
        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(collector, times(count)).emit(captor.capture());
        List<String> values = TupleUtils.unwrap(captor.getAllValues(), 0);
        assertEquals(packets, values);
    }
}
