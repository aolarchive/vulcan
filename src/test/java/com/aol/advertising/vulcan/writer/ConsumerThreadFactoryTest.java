package com.aol.advertising.vulcan.writer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import org.junit.Test;
import org.mockito.Mock;

import com.aol.advertising.vulcan.ConfiguredUnitTest;
import com.aol.advertising.vulcan.writer.ConsumerThreadFactory;

public class ConsumerThreadFactoryTest extends ConfiguredUnitTest {

  @Mock
  private Runnable runnableMock;

  @Test
  public void whenAThreadIsRetrieved_thenItHasTheExpectedNameForTheDisruptorConsumerThread() {
    Thread consumerThread = new ConsumerThreadFactory().newThread(runnableMock);

    assertThat(consumerThread.getName(), is(equalTo("vulcan-avro-writer")));
  }
}
