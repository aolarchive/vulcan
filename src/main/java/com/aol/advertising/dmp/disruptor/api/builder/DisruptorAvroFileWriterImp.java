package com.aol.advertising.dmp.disruptor.api.builder;

import org.apache.avro.specific.SpecificRecord;

import com.aol.advertising.dmp.disruptor.api.DisruptorAvroFileWriter;
import com.aol.advertising.dmp.disruptor.ringbuffer.BufferEvent;
import com.lmax.disruptor.dsl.Disruptor;

class DisruptorAvroFileWriterImp implements DisruptorAvroFileWriter {

  Disruptor<BufferEvent> disruptor;

  DisruptorAvroFileWriterImp() {}

  @Override
  public void write(SpecificRecord avroRecord) {

  }

  @Override
  public void close() throws Exception {
    // close executor with awaitTermination
    // flush writer
    // close disruptor preventing event publishing to the ringbuffer with a volatile flag
  }
}
