package com.aol.advertising.dmp.disruptor.api;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aol.advertising.dmp.disruptor.api.builder.steps.AvroFileNameStep;
import com.aol.advertising.dmp.disruptor.api.builder.steps.AvroSchemaStep;
import com.aol.advertising.dmp.disruptor.api.builder.steps.OptionalSteps;
import com.aol.advertising.dmp.disruptor.api.builder.steps.Steps;
import com.aol.advertising.dmp.disruptor.exception.DisruptorExceptionHandler;
import com.aol.advertising.dmp.disruptor.ringbuffer.AvroEvent;
import com.aol.advertising.dmp.disruptor.ringbuffer.AvroEventFactory;
import com.aol.advertising.dmp.disruptor.rolling.TimeAndSizeBasedRollingPolicy;
import com.aol.advertising.dmp.disruptor.writer.AvroEventConsumer;
import com.aol.advertising.dmp.disruptor.writer.AvroEventPublisher;
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
  private static final AvroEventFactory bufferEventFactory = new AvroEventFactory();
  private static final ExecutorService consumerExecutor = Executors.newSingleThreadExecutor();

  private AvroEventPublisher publisherUnderConstruction;
  
  private File avroFileName;
  private Schema avroSchema;
  private int ringBufferSize;
  private ProducerType producerType;
  private WaitStrategy waitStrategy;
  private RollingPolicy rollingPolicy;

  private DisruptorAvroFileWriterBuilder() {
    publisherUnderConstruction = new AvroEventPublisher();
  }

  /**
   * Start creating a {@link DisruptorAvroFileWriter}
   */
  public static AvroFileNameStep createNewWriter() {
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
  public AvroSchemaStep thatWritesTo(final File avroFileName) throws IllegalArgumentException {
    validateFile(avroFileName);
    this.avroFileName = avroFileName;
    initDefaultRollingPolicy(this.avroFileName);
    return this;
  }
  
  private void validateFile(final File avroFileName) throws IllegalArgumentException {
    if (avroFileName == null) {
      throw new IllegalArgumentException("Specified Avro file was null");
    }
    if (avroFileName.exists()) {
      validateFileIsNotADir(avroFileName);
      validateFilePermissions(avroFileName);
    } else {
      final File parentDirName = avroFileName.getParentFile();
      if (parentDirName.exists() && parentDirName.isDirectory()) {
        validateDirPermissions(parentDirName);
      } else {
        createParentDir(parentDirName);
      }
    }
  }

  private void validateFileIsNotADir(final File avroFileName) throws IllegalArgumentException {
    if (avroFileName.isDirectory()) {
      throw new IllegalArgumentException("Specified Avro file exists and it is a directory");
    }
  }

  private void validateFilePermissions(final File avroFileName) throws IllegalArgumentException {
    if (!avroFileName.canRead()) {
      throw new IllegalArgumentException("Specified Avro file needs to be readable");
    }
    if (!avroFileName.canWrite()) {
      throw new IllegalArgumentException("Specified Avro file needs to be writable");
    }
  }

  private void validateDirPermissions(final File parentDirName) throws IllegalArgumentException {
    if (!parentDirName.canRead()) {
      throw new IllegalArgumentException("Parent directory of specified Avro file needs to be readable");
    }
    if (!parentDirName.canWrite()) {
      throw new IllegalArgumentException("Parent directory of specified Avro file needs to be writable");
    }
    if (!parentDirName.canExecute()) {
      throw new IllegalArgumentException("Parent directory of specified Avro file needs to be executable");
    }
  }

  private void createParentDir(final File parentDirName) throws IllegalArgumentException {
    try {
      FileUtils.forceMkdir(parentDirName);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private void initDefaultRollingPolicy(final File avroFileName) {
    rollingPolicy = new TimeAndSizeBasedRollingPolicy(50, 9, avroFileName);
  }

  @Override
  public OptionalSteps thatWritesRecordsOf(final Schema avroSchema) throws IllegalArgumentException {
    if (avroSchema == null) {
      throw new IllegalArgumentException("Specified Avro schema was null");
    }
    this.avroSchema = avroSchema;
    return this;
  }

  @Override
  public OptionalSteps withRingBufferSize(int ringBufferSize) {
    this.ringBufferSize = ringBufferSize;
    return this;
  }

  @Override
  public OptionalSteps withProducerType(final ProducerType producerType) {
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
    publisherUnderConstruction.registerConsumerExecutorForShutdown(consumerExecutor);
    publisherUnderConstruction.startPublisherUsing(buildDisruptor());
    return publisherUnderConstruction;
  }

  @SuppressWarnings("unchecked")
  private Disruptor<AvroEvent> buildDisruptor() {
    final Disruptor<AvroEvent> disruptor = new Disruptor<>(bufferEventFactory,
                                                           ringBufferSize,
                                                           consumerExecutor,
                                                           producerType,
                                                           waitStrategy);
    disruptor.handleExceptionsWith(new DisruptorExceptionHandler());
    disruptor.handleEventsWith(new AvroEventConsumer(avroFileName, avroSchema, rollingPolicy));
    return disruptor;
  }
}
