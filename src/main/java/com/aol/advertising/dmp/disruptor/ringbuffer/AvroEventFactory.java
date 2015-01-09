package com.aol.advertising.dmp.disruptor.ringbuffer;

import com.lmax.disruptor.EventFactory;

public class AvroEventFactory implements EventFactory<AvroEvent> {

  @Override
  public AvroEvent newInstance() {
    return new AvroEvent();
  }

}
