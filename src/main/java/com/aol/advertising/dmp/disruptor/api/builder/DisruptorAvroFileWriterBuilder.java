package com.aol.advertising.dmp.disruptor.api.builder;

import java.io.File;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aol.advertising.dmp.disruptor.DisruptorAvroFileWriter;
import com.aol.advertising.dmp.disruptor.ringbuffer.BufferEventFactory;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Fluent builder API to create new instances of {@link DisruptorAvroFileWriter}. This API is
 * suitable for standalone applications with no dependency injection and for programmatic
 * configuration styles such as Spring's Java-based configuration.
 * <p>
 * See:
 * <ul>
 * <li><a href="http://en.wikipedia.org/wiki/Fluent_interface">Fluent API</li>
 * <li><a href="http://rdafbn.blogspot.com/2012/07/step-builder-pattern_28.html">Step builder
 * pattern</li>
 * </ul>
 */
public class DisruptorAvroFileWriterBuilder implements MandatorySteps, OptionalSteps {

  private static final Logger log = LoggerFactory.getLogger(DisruptorAvroFileWriterBuilder.class);
  private static final BufferEventFactory bufferEventFactory = new BufferEventFactory();

  private DisruptorAvroFileWriterImp writerUnderConstruction;
  
  private File avroFilename;
  private int ringBufferSize;
  private ProducerType producerType;
  private WaitStrategy waitStrategy;

  private DisruptorAvroFileWriterBuilder() {
    writerUnderConstruction = new DisruptorAvroFileWriterImp();
  }

  /**
   * Start creating a {@link DisruptorAvroFileWriter}
   */
  public static MandatorySteps createNewWriter() {
    final DisruptorAvroFileWriterBuilder newBuilder = new DisruptorAvroFileWriterBuilder();
    newBuilder.useSensibleDefaults();
    return newBuilder;
  }

  private void useSensibleDefaults() {
    ringBufferSize = 1024;
    producerType = ProducerType.MULTI;
    waitStrategy = new SleepingWaitStrategy();
  }

  @Override
  public OptionalSteps thatWritesTo(final File avroFilename) throws IllegalArgumentException {
    validateFile(avroFilename);
    this.avroFilename = avroFilename;
    return this;
  }

  private void validateFile(final File avroFilename) {
    if (avroFilename == null) {
      throw new IllegalArgumentException("Specified Avro file was null");
    }
    if (!avroFilename.canRead()) {
      throw new IllegalArgumentException("Specified Avro file needs to be readable");
    }
    if (!avroFilename.canWrite()) {
      throw new IllegalArgumentException("Specified Avro file needs to be writable");
    }
  }

  @Override
  public OptionalSteps withARingBufferOfSize(int ringBufferSize) {
    this.ringBufferSize = ringBufferSize;
    return this;
  }

  @Override
  public OptionalSteps withAProducerOfType(final ProducerType producerType) {
    if (producerType != null) {
      this.producerType = producerType;
    } else {
      log.warn("Tried to configure producer type with a null value");
    }
    return this;
  }

  @Override
  public OptionalSteps withWaitStrategy(final WaitStrategy waitStrategy) {
    if (waitStrategy != null) {
      this.waitStrategy = waitStrategy;
    } else {
      log.warn("Tried to configure waiting strategy with a null value");
    }
    this.waitStrategy = waitStrategy;
    return this;
  }

  @Override
  public DisruptorAvroFileWriter build() {
    writerUnderConstruction.disruptor = new Disruptor<>(bufferEventFactory,
                                                        ringBufferSize,
                                                        Executors.newSingleThreadExecutor(),
                                                        producerType,
                                                        waitStrategy);
    //config handlers y tal
    return writerUnderConstruction;
  }

}
