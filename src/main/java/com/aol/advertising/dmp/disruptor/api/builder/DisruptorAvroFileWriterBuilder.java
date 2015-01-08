package com.aol.advertising.dmp.disruptor.api.builder;

import java.io.File;
import java.util.concurrent.Executors;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aol.advertising.dmp.disruptor.DisruptorAvroFileWriter;
import com.aol.advertising.dmp.disruptor.api.builder.steps.AvroSchemaStep;
import com.aol.advertising.dmp.disruptor.api.builder.steps.MandatorySteps;
import com.aol.advertising.dmp.disruptor.api.builder.steps.OptionalSteps;
import com.aol.advertising.dmp.disruptor.api.builder.steps.Steps;
import com.aol.advertising.dmp.disruptor.ringbuffer.BufferEventFactory;
import com.aol.advertising.dmp.disruptor.rolling.RollingPolicy;
import com.aol.advertising.dmp.disruptor.rolling.TimeAndSizeBasedRollingPolicy;
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
public class DisruptorAvroFileWriterBuilder implements Steps {

  private static final Logger log = LoggerFactory.getLogger(DisruptorAvroFileWriterBuilder.class);
  private static final BufferEventFactory bufferEventFactory = new BufferEventFactory();

  private DisruptorAvroFileWriterImp writerUnderConstruction;
  
  private File avroFileName;
  private Schema avroSchema;
  private int ringBufferSize;
  private ProducerType producerType;
  private WaitStrategy waitStrategy;
  private RollingPolicy rollingPolicy;

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
    rollingPolicy = new TimeAndSizeBasedRollingPolicy(50);
  }

  @Override
  public AvroSchemaStep thatWritesTo(final File avroFileName) throws IllegalArgumentException {
    validateFile(avroFileName);
    this.avroFileName = avroFileName;
    return this;
  }
  
  private void validateFile(final File avroFileName) {
    if (avroFileName == null) {
      throw new IllegalArgumentException("Specified Avro file was null");
    }
    if (!avroFileName.canRead()) {
      throw new IllegalArgumentException("Specified Avro file needs to be readable");
    }
    if (!avroFileName.canWrite()) {
      throw new IllegalArgumentException("Specified Avro file needs to be writable");
    }
  }

  @Override
  public OptionalSteps thatWritesRecordsWith(final Schema avroSchema) throws IllegalArgumentException {
    if (avroSchema == null) {
      throw new IllegalArgumentException("Specified Avro schema was null");
    }
    this.avroSchema = avroSchema;
    return this;
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
      log.warn("Tried to configure the producer type with a null value");
    }
    return this;
  }

  @Override
  public OptionalSteps withWaitStrategy(final WaitStrategy waitStrategy) {
    if (waitStrategy != null) {
      this.waitStrategy = waitStrategy;
    } else {
      log.warn("Tried to configure the waiting strategy with a null value");
    }
    return this;
  }

  @Override
  public OptionalSteps withRollingPolicy(final RollingPolicy rollingPolicy) {
    if (rollingPolicy != null) {
      this.rollingPolicy = rollingPolicy;
    } else {
      log.warn("Tried to configure the file rolling policy with a null value");
    }
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
