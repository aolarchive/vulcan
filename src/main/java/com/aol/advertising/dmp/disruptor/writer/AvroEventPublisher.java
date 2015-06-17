package com.aol.advertising.dmp.disruptor.writer;

import java.util.concurrent.ExecutorService;

import org.apache.avro.specific.SpecificRecord;

import com.aol.advertising.dmp.disruptor.api.DisruptorAvroFileWriter;
import com.aol.advertising.dmp.disruptor.ringbuffer.AvroEvent;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.dsl.Disruptor;

public class AvroEventPublisher implements DisruptorAvroFileWriter, EventTranslatorOneArg<AvroEvent, SpecificRecord> {

  private volatile boolean disruptorIsRunning;
  private Disruptor<AvroEvent> disruptor;
  private ExecutorService consumerExecutor;

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
  public void translateTo(final AvroEvent avroEvent, long sequence, final SpecificRecord avroRecord) {
    avroEvent.setAvroRecord(avroRecord);
  }

  /*
   * Call to shutdown may never return if publishing has not stopped before calling, thus the need
   * for the volatile flag. See com.lmax.disruptor.dsl.Disruptor#shutdown()
   */
  @Override
  public void close() throws Exception {
    disruptor.shutdown();
    consumerExecutor.shutdown();
    disruptorIsRunning = false;
  }

  public void registerConsumerExecutorForShutdown(final ExecutorService consumerExecutor) {
    this.consumerExecutor = consumerExecutor;
  }

  public void startPublisherUsing(final Disruptor<AvroEvent> fullyConfiguredDisruptor) {
    disruptor = fullyConfiguredDisruptor;
    disruptor.start();
    disruptorIsRunning = true;
  }
}
