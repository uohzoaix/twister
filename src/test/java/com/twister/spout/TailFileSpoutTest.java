package com.twister.spout;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.IRichSpout;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twister.TupleEmitter;
import com.twister.utils.TupleUtils;
import com.twister.utils.FileUtils;
import com.twister.utils.TimeUtils;
 

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TailFileSpoutTest {

    private static final Logger LOG = LoggerFactory.getLogger(TailFileSpoutTest.class);

    private IRichSpout spout;

    @Mock
    private SpoutOutputCollector collector;

    @Test
    public void tailOnFile() throws Exception {
        // Given
        int count = 10;
        int delay = 10;

        File file = FileUtils.createTempFile();
        spout = new TailFileSpout(file.getAbsolutePath(), delay); // Tail every 10 ms

        CountDownLatch done = new CountDownLatch(1);
        TupleEmitter emitter = new TupleEmitter(spout, collector, done, count);
        emitter.start(); // Start the thread who calls nextTuple

        // When
        List<String> lines = FileUtils.writeRandomLines(file, count, 100, MILLISECONDS);// Write 10 lines in the file each 100 ms

        // Then
        assertTrue(done.await(5, SECONDS));
        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(collector, times(count)).emit(captor.capture());
        List<String> values = TupleUtils.unwrap(captor.getAllValues(), 0);
        assertEquals(lines, values);
    }


    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfFilepathIsADirectory() throws Exception {
        String directory = System.getProperty("java.io.tmpdir");
        spout = new TailFileSpout(directory);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfFileIsADirectory() throws Exception {
        String directory = System.getProperty("java.io.tmpdir");
        spout = new TailFileSpout(new File(directory));
    }


    // Doesn't work
    @Ignore
    @Test
    public void tailOnFileRenamed() throws Exception {
        // Given
        int count = 10;
        int numberOfLines = count / 2;
        int delay = 10;

        File file = FileUtils.createTempFile();
        spout = new TailFileSpout(file.getAbsolutePath(), delay); // Tail every 10 ms

        CountDownLatch done = new CountDownLatch(1);
        TupleEmitter emitter = new TupleEmitter(spout, collector, done, count);
        emitter.start(); // Start the thread who calls nextTuple

        // When
        List<String> lines = FileUtils.writeRandomLines(file, numberOfLines);

        TimeUtils.sleep(2, SECONDS);

        // Now rename the file
        File fileRenamed = FileUtils.createTempFile();
        assertTrue(file.renameTo(fileRenamed));

        // Write line in the new file
        List<String> otherLines = FileUtils.writeRandomLines(file, numberOfLines);

        // Then
        assertTrue(done.await(5, SECONDS));
        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(collector, times(count)).emit(captor.capture());
        List<String> values = TupleUtils.unwrap(captor.getAllValues(), 0);

        List<String> allLines = new ArrayList<String>(lines);
        allLines.addAll(otherLines);
        assertEquals(allLines, values);
    }

    @Test
    @Ignore
    public void tailOnUnixPipe() throws Exception {
        // implement this case
    }

}
