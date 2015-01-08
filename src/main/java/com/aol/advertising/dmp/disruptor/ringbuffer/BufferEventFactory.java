package com.aol.advertising.dmp.disruptor.ringbuffer;

import com.lmax.disruptor.EventFactory;

public class BufferEventFactory implements EventFactory<BufferEvent> {

  @Override
  public BufferEvent newInstance() {
    return null;
  }

}
