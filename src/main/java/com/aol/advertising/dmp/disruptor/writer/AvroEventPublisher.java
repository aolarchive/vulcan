package com.aol.advertising.dmp.disruptor.writer;

import org.apache.avro.specific.SpecificRecord;

import com.aol.advertising.dmp.disruptor.api.DisruptorAvroFileWriter;
import com.aol.advertising.dmp.disruptor.ringbuffer.AvroEvent;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.dsl.Disruptor;

public class AvroEventPublisher implements DisruptorAvroFileWriter, EventTranslatorOneArg<AvroEvent, SpecificRecord> {

  private volatile boolean disruptorIsRunning;
  private Disruptor<AvroEvent> disruptor;

  public AvroEventPublisher() {
    disruptorIsRunning = false;
  }

  @Override
  public void write(final SpecificRecord avroRecord) {
    if (disruptorIsRunning) {
      publishRecordToBuffer(avroRecord);
    }
  }

  private void publishRecordToBuffer(final SpecificRecord avroRecord) {
    disruptor.publishEvent(this, avroRecord);
  }

  @Override
  public void translateTo(final AvroEvent avroEvent, long _, final SpecificRecord avroRecord) {
    avroEvent.setAvroRecord(avroRecord);
  }

  /*
   * Call to shutdown may never return if publishing has not stopped before calling, thus the need
   * for the flag. See com.lmax.disruptor.dsl.Disruptor#shutdown()
   */
  @Override
  public void close() throws Exception {
    disruptorIsRunning = false;
    disruptor.shutdown();
  }

  public void startPublisherUsing(final Disruptor<AvroEvent> fullyConfiguredDisruptor) {
    disruptor = fullyConfiguredDisruptor;
    disruptor.start();
    disruptorIsRunning = true;
  }
}
