package com.aol.advertising.dmp.disruptor.writer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import org.junit.Test;
import org.mockito.Mock;

import com.aol.advertising.dmp.disruptor.ConfiguredUnitTest;

public class ConsumerThreadFactoryTest extends ConfiguredUnitTest {

  @Mock
  private Runnable runnableMock;

  @Test
  public void whenAThreadIsRetrieved_thenItHasTheExpectedNameForTheDisruptorConsumerThread() {
    final Thread consumerThread = new ConsumerThreadFactory().newThread(runnableMock);

    assertThat(consumerThread.getName(), is(equalTo("disruptor-avro-writer")));
  }
}
