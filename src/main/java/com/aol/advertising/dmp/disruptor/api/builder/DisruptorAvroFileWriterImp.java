package com.aol.advertising.dmp.disruptor.api.builder;

import org.apache.avro.specific.SpecificRecord;

import com.aol.advertising.dmp.disruptor.DisruptorAvroFileWriter;
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
    // cerrar executor con awaitTermination
    // hacer flush
    // cerrar disruptor evitando publishes en el ringbuffer con un flag volatil
  }
}
