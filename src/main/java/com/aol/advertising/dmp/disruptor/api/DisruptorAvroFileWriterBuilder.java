package com.aol.advertising.dmp.disruptor.api;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.avro.Schema;
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

  private static final AvroEventFactory avroEventFactory = new AvroEventFactory();

  private final ExecutorService consumerExecutor;
  private final AvroEventPublisher publisherUnderConstruction;
  
  private Path avroFileName;
  private Schema avroSchema;
  private int ringBufferSize;
  private ProducerType producerType;
  private WaitStrategy waitStrategy;
  private RollingPolicy rollingPolicy;

  private DisruptorAvroFileWriterBuilder() {
    publisherUnderConstruction = new AvroEventPublisher();
    consumerExecutor = Executors.newSingleThreadExecutor();
  }

  /**
   * Start creating a {@link DisruptorAvroFileWriter}
   */
  public static AvroFileNameStep startCreatingANewWriter() {
    final DisruptorAvroFileWriterBuilder newBuilder = new DisruptorAvroFileWriterBuilder();
    newBuilder.useSensibleDefaults();
    return newBuilder;
  }

  private void useSensibleDefaults() {
    ringBufferSize = 2048;
    producerType = ProducerType.MULTI;
    waitStrategy = new SleepingWaitStrategy();
  }

  @Override
  public AvroSchemaStep thatWritesTo(final Path avroFileName) {
    this.avroFileName = avroFileName;
    validateFile();
    initDefaultRollingPolicy();
    return this;
  }
  
  private void validateFile() {
    if (avroFileName == null) {
      throw new IllegalArgumentException("Specified Avro file was null");
    }
    if (Files.exists(avroFileName)) {
      validateFileIsNotADir();
      validateFilePermissions();
    } else {
      validateDirPermissions();
    }
  }

  private void validateFileIsNotADir() {
    if (Files.isDirectory(avroFileName)) {
      throw new IllegalArgumentException("Specified Avro file exists and it is a directory");
    }
  }

  private void validateFilePermissions() {
    if (!Files.isReadable(avroFileName)) {
      throw new IllegalArgumentException("Specified Avro file needs to be readable");
    }
    if (!Files.isWritable(avroFileName)) {
      throw new IllegalArgumentException("Specified Avro file needs to be writable");
    }
  }

  private void validateDirPermissions() {
    final Path directory = avroFileName.getParent();
    if (!Files.isReadable(directory)) {
      throw new IllegalArgumentException("Target directory for the specified Avro file needs to exist and be readable");
    }
    if (!Files.isWritable(directory)) {
      throw new IllegalArgumentException("Target directory for the specified Avro file needs to exist and be writable");
    }
    if (!Files.isExecutable(directory)) {
      throw new IllegalArgumentException("Target directory for the specified Avro file needs to exist and be executable");
    }
  }

  private void initDefaultRollingPolicy() {
    try {
      tryToInitDefaultRollingPolicy();
    } catch (IOException ioe) {
      throw new IllegalArgumentException(ioe);
    }
  }

  private void tryToInitDefaultRollingPolicy() throws IOException {
    rollingPolicy = new TimeAndSizeBasedRollingPolicy(50, avroFileName);
  }

  @Override
  public OptionalSteps thatWritesRecordsOf(final Schema avroSchema) {
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
  public DisruptorAvroFileWriter createNewWriter() {
    publisherUnderConstruction.registerConsumerExecutorForShutdown(consumerExecutor);
    publisherUnderConstruction.startPublisherUsing(buildDisruptor());
    return publisherUnderConstruction;
  }

  @SuppressWarnings("unchecked")
  private Disruptor<AvroEvent> buildDisruptor() {
    final Disruptor<AvroEvent> disruptor = new Disruptor<>(avroEventFactory,
                                                           ringBufferSize,
                                                           consumerExecutor,
                                                           producerType,
                                                           waitStrategy);
    disruptor.handleExceptionsWith(new DisruptorExceptionHandler());
    disruptor.handleEventsWith(new AvroEventConsumer(avroFileName, avroSchema, rollingPolicy));
    return disruptor;
  }
}
