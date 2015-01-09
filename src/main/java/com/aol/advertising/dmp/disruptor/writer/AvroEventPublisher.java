package com.aol.advertising.dmp.disruptor.writer;

import org.apache.avro.specific.SpecificRecord;

import com.aol.advertising.dmp.disruptor.api.DisruptorAvroFileWriter;
import com.aol.advertising.dmp.disruptor.ringbuffer.AvroEvent;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

public class AvroEventPublisher implements DisruptorAvroFileWriter, EventTranslatorOneArg<AvroEvent, SpecificRecord> {

  private RingBuffer<AvroEvent> avroRingBuffer;

  public AvroEventPublisher() {}

  @Override
  public void write(final SpecificRecord avroRecord) {
    publishRecordToBuffer(avroRecord);
  }

  private void publishRecordToBuffer(final SpecificRecord avroRecord) {
    avroRingBuffer.publishEvent(this, avroRecord);
  }

  @Override
  public void translateTo(final AvroEvent avroEvent, long _, final SpecificRecord avroRecord) {
    avroEvent.setAvroRecord(avroRecord);
  }

  @Override
  public void close() throws Exception {
    // close executor with awaitTermination
    // flush writer
    // close disruptor preventing event publishing to the ringbuffer with a volatile flag
  }

  public void startPublisherUsing(final Disruptor<AvroEvent> fullyConfiguredDisruptor) {
    avroRingBuffer = fullyConfiguredDisruptor.start();
  }
}
