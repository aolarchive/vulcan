package com.aol.advertising.vulcan.writer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.avro.specific.SpecificRecord;

import com.aol.advertising.vulcan.api.AvroWriter;
import com.aol.advertising.vulcan.ringbuffer.AvroEvent;
import com.aol.advertising.vulcan.ringbuffer.AvroEventFactory;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.dsl.Disruptor;

public class AvroEventPublisher implements AvroWriter, EventTranslatorOneArg<AvroEvent, SpecificRecord> {

  private Disruptor<AvroEvent> disruptor;
  private ExecutorService consumerExecutor;

  public AvroEventPublisher() {
    disruptor = getNoOpDisruptor();
  }

  private Disruptor<AvroEvent> getNoOpDisruptor() {
    return new Disruptor<>(new AvroEventFactory(), 1, Executors.newSingleThreadExecutor());
  }

  @Override
  public void write(SpecificRecord avroRecord) {
    publishRecordToBuffer(avroRecord);
  }

  private void publishRecordToBuffer(SpecificRecord avroRecord) {
    disruptor.publishEvent(this, avroRecord);
  }

  @Override
  public void translateTo(AvroEvent avroEvent, long sequence, SpecificRecord avroRecord) {
    avroEvent.setAvroRecord(avroRecord);
  }

  /*
   * Call to shutdown may never return if publishing has not stopped before calling. See
   * com.lmax.disruptor.dsl.Disruptor#shutdown()
   */
  @Override
  public void close() throws Exception {
    disruptor.shutdown();
    consumerExecutor.shutdown();
    disruptor = getNoOpDisruptor();
  }

  public void registerConsumerExecutorForShutdown(ExecutorService consumerExecutor) {
    this.consumerExecutor = consumerExecutor;
  }

  public void startPublisherUsing(Disruptor<AvroEvent> fullyConfiguredDisruptor) {
    disruptor = fullyConfiguredDisruptor;
    disruptor.start();
  }
}
