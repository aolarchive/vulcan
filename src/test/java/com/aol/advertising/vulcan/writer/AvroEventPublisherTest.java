package com.aol.advertising.vulcan.writer;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ExecutorService;

import org.apache.avro.specific.SpecificRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import com.aol.advertising.vulcan.ConfiguredUnitTest;
import com.aol.advertising.vulcan.ringbuffer.AvroEvent;
import com.aol.advertising.vulcan.writer.AvroEventPublisher;
import com.lmax.disruptor.dsl.Disruptor;

public class AvroEventPublisherTest extends ConfiguredUnitTest {

  private AvroEventPublisher avroEventPublisherUnderTest;

  @Mock
  private Disruptor<AvroEvent> disruptorMock;
  @Mock
  private ExecutorService consumerExecutorMock;
  @Mock
  private AvroEvent avroEventMock;
  @Mock
  private SpecificRecord avroRecordMock;

  @Before
  public void setUp() throws Exception {
    avroEventPublisherUnderTest = new AvroEventPublisher();
    avroEventPublisherUnderTest.registerConsumerExecutorForShutdown(consumerExecutorMock);
  }

  @Test
  public void whenAnAvroRecordIsWritten_andThePublisherHasNotBeenStarted_thenTheRecordIsNotPublished() {
    avroEventPublisherUnderTest.write(avroRecordMock);

    thenTheRecordIsNotPublished();
  }

  @Test
  public void whenAnAvroRecordIsWritten_andThePublisherHasBeenShutdown_thenTheRecordIsNotPublished() throws Exception {
    givenThePublisherHasBeenStarted();
    givenThePublisherHasBeenShutdown();

    avroEventPublisherUnderTest.write(avroRecordMock);

    thenTheRecordIsNotPublished();
  }

  @Test
  public void whenAnAvroRecordIsWritten_andThePublisherHasBeenStarted_thenTheRecordIsPublished() {
    givenThePublisherHasBeenStarted();
    
    avroEventPublisherUnderTest.write(avroRecordMock);

    thenTheRecordIsPublished();
  }
  
  @Test
  public void whenAnAvroRecordIsWritten_thenThePublishedEventContainsTheRecord() {
    givenThePublisherHasBeenStarted();
    
    avroEventPublisherUnderTest.translateTo(avroEventMock, -1, avroRecordMock);
    
    verify(avroEventMock).setAvroRecord(avroRecordMock);
  }

  @Test
  public void whenAPublisherIsShutdown_thenAllResourcesAreShutdown() throws Exception {
    givenThePublisherHasBeenStarted();

    avroEventPublisherUnderTest.close();

    thenAllResourcesAreShutdown();
  }
  
  @Test
  public void whenAPublisherIsStarted_thenDisruptorIsStarted() {
    avroEventPublisherUnderTest.startPublisherUsing(disruptorMock);
    
    verify(disruptorMock).start();
  }

  private void givenThePublisherHasBeenStarted() {
    avroEventPublisherUnderTest.startPublisherUsing(disruptorMock);
  }

  private void givenThePublisherHasBeenShutdown() throws Exception {
    avroEventPublisherUnderTest.close();
  }

  private void thenTheRecordIsNotPublished() {
    verify(disruptorMock, never()).publishEvent(avroEventPublisherUnderTest, avroRecordMock);
  }

  private void thenTheRecordIsPublished() {
    verify(disruptorMock).publishEvent(avroEventPublisherUnderTest, avroRecordMock);
  }

  private void thenAllResourcesAreShutdown() {
    verify(disruptorMock).shutdown();
    verify(consumerExecutorMock).shutdown();
  }
}
