package com.aol.advertising.dmp.disruptor.ringbuffer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

import org.junit.Test;

public class AvroEventFactoryTest {

  @Test
  public void whenTheFactoryMethodIsCalled_thenAnInstanceOfAvroEventIsReturned() {
    assertThat(new AvroEventFactory().newInstance(), instanceOf(AvroEvent.class));
  }

}
