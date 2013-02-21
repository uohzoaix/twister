package com.twister.spout;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.twister.TupleEmitter;
import com.twister.utils.SocketUtils;
import com.twister.utils.TupleUtils;
import com.twister.utils.FileUtils;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TextFileSpoutTest {

    private IRichSpout spout;
    private File file;

    @Mock
    private Map<Object, Object> stormConf;
    @Mock
    private TopologyContext topologyContext;
    @Mock
    private SpoutOutputCollector collector;

    @Before
    public void setUp() throws Exception {
        file = FileUtils.createTempFile();
        file.deleteOnExit();
        // Create and open spout
        spout = new TextFileSpout(file);
        spout.open(stormConf, topologyContext, collector);
    }

    @Test
    public void tailTextFile() throws Exception {
        // Given
        int count = 10;
        CountDownLatch done = new CountDownLatch(1);
        List<String> lines = FileUtils.writeRandomLines(file, count);

        // When
        TupleEmitter emitter = new TupleEmitter(spout, collector, done, 10); // Call next tuple
        emitter.start(); // Start the thread who calls nextTuple

        // Then
        assertTrue(done.await(5, SECONDS));

        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(collector, times(count)).emit(captor.capture());
        List<String> values = TupleUtils.unwrap(captor.getAllValues(), 0);
        assertEquals(lines,values);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfFilepathIsADirectory() throws Exception {
        String directory = System.getProperty("java.io.tmpdir");
        spout = new TextFileSpout(directory);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfFileIsADirectory() throws Exception {
        String directory = System.getProperty("java.io.tmpdir");
        spout = new TextFileSpout(new File(directory));
    }


    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionIfFileIsClosed() throws Exception {
        // Given
        spout.close();

        // When
        spout.nextTuple();

        // Then IllegalStateException thrown
    }

}
