package com.aol.advertising.vulcan.ringbuffer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

import org.junit.Test;

import com.aol.advertising.vulcan.ringbuffer.AvroEvent;
import com.aol.advertising.vulcan.ringbuffer.AvroEventFactory;

public class AvroEventFactoryTest {

  @Test
  public void whenTheFactoryMethodIsCalled_thenAnInstanceOfAvroEventIsReturned() {
    assertThat(new AvroEventFactory().newInstance(), instanceOf(AvroEvent.class));
  }

}
