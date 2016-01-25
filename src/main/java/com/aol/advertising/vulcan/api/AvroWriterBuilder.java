package com.aol.advertising.vulcan.api;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aol.advertising.vulcan.api.builder.steps.AvroFilenameStep;
import com.aol.advertising.vulcan.api.builder.steps.AvroSchemaStep;
import com.aol.advertising.vulcan.api.builder.steps.OptionalSteps;
import com.aol.advertising.vulcan.api.builder.steps.Steps;
import com.aol.advertising.vulcan.api.rolling.RollingPolicy;
import com.aol.advertising.vulcan.exception.DisruptorExceptionHandler;
import com.aol.advertising.vulcan.ringbuffer.AvroEvent;
import com.aol.advertising.vulcan.ringbuffer.AvroEventFactory;
import com.aol.advertising.vulcan.rolling.TimeAndSizeBasedRollingPolicy;
import com.aol.advertising.vulcan.rolling.TimeAndSizeBasedRollingPolicyConfig;
import com.aol.advertising.vulcan.writer.AvroEventConsumer;
import com.aol.advertising.vulcan.writer.AvroEventPublisher;
import com.aol.advertising.vulcan.writer.ConsumerThreadFactory;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Fluent builder API to create new instances of {@link AvroWriter}. This API is suitable for
 * standalone applications with no dependency injection and for programmatic configuration styles
 * such as Spring's Java-based configuration.
 * <p>
 * See:
 * <ul>
 * <li><a href="http://en.wikipedia.org/wiki/Fluent_interface">Fluent API</a></li>
 * <li><a href="http://rdafbn.blogspot.com/2012/07/step-builder-pattern_28.html">Step builder
 * pattern</a></li>
 * </ul>
 * </p>
 * 
 * @author Jaime Nuche
 *
 */
public class AvroWriterBuilder implements Steps {

  private static final Logger log = LoggerFactory.getLogger(AvroWriterBuilder.class);
  private static final AvroEventFactory avroEventFactory = new AvroEventFactory();
  private static final ThreadFactory consumerExecutorThreadFactory = new ConsumerThreadFactory();

  private final ExecutorService consumerExecutor;
  private final AvroEventPublisher publisherUnderConstruction;
  
  private Path avroFilename;
  private Schema avroSchema;
  private int ringBufferSize;
  private ProducerType producerType;
  private WaitStrategy waitStrategy;
  private RollingPolicy rollingPolicy;

  private AvroWriterBuilder() {
    publisherUnderConstruction = new AvroEventPublisher();
    consumerExecutor = Executors.newSingleThreadExecutor(consumerExecutorThreadFactory);
  }

  /**
   * Start creating a {@link AvroWriter}
   */
  public static AvroFilenameStep startCreatingANewWriter() {
    AvroWriterBuilder newBuilder = new AvroWriterBuilder();
    newBuilder.useSensibleDefaults();
    return newBuilder;
  }

  private void useSensibleDefaults() {
    ringBufferSize = 2048;
    producerType = ProducerType.MULTI;
    waitStrategy = new SleepingWaitStrategy();
  }

  @Override
  public AvroSchemaStep thatWritesTo(Path avroFilename) {
    this.avroFilename = avroFilename;
    validateFile();
    initRollingPolicyWithDefaultConfiguration();
    return this;
  }

  private void validateFile() {
    if (avroFilename == null) {
      throw new NullPointerException("Specified Avro file was null");
    }
    if (Files.exists(avroFilename)) {
      validateFileIsNotADir();
      validateFilePermissions();
    } else {
      validateDirPermissions();
    }
  }

  private void validateFileIsNotADir() {
    if (Files.isDirectory(avroFilename)) {
      throw new IllegalArgumentException("Specified Avro file exists and it is a directory");
    }
  }

  private void validateFilePermissions() {
    if (!Files.isReadable(avroFilename)) {
      throw new IllegalArgumentException("Specified Avro file needs to be readable");
    }
    if (!Files.isWritable(avroFilename)) {
      throw new IllegalArgumentException("Specified Avro file needs to be writable");
    }
  }

  private void validateDirPermissions() {
    Path directory = avroFilename.getParent();
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

  private void initRollingPolicyWithDefaultConfiguration() {
    rollingPolicy = new TimeAndSizeBasedRollingPolicy(new TimeAndSizeBasedRollingPolicyConfig());
  }

  @Override
  public AvroSchemaStep thatWritesTo(String avroFilename) {
    if (avroFilename == null) {
      throw new NullPointerException("Specified Avro file was null");
    }
    return thatWritesTo(Paths.get(avroFilename));
  }

  @Override
  public OptionalSteps thatWritesRecordsOf(Schema avroSchema) {
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
  public OptionalSteps withProducerType(ProducerType producerType) {
    if (producerType != null) {
      this.producerType = producerType;
    } else {
      log.warn("Tried to configure the producer type with a null value");
    }
    return this;
  }

  @Override
  public OptionalSteps withWaitStrategy(WaitStrategy waitStrategy) {
    if (waitStrategy != null) {
      this.waitStrategy = waitStrategy;
    } else {
      log.warn("Tried to configure the waiting strategy with a null value");
    }
    return this;
  }

  @Override
  public OptionalSteps withRollingPolicy(RollingPolicy rollingPolicy) {
    if (rollingPolicy != null) {
      this.rollingPolicy = rollingPolicy;
    } else {
      log.warn("Tried to configure the file rolling policy with a null value");
    }
    return this;
  }

  @Override
  public OptionalSteps withDefaultRollingPolicyConfiguration(TimeAndSizeBasedRollingPolicyConfig configuration) {
    rollingPolicy = new TimeAndSizeBasedRollingPolicy(configuration);
    return this;
  }

  @Override
  public AvroWriter createNewWriter() {
    rollingPolicy.registerAvroFilename(avroFilename);
    publisherUnderConstruction.registerConsumerExecutorForShutdown(consumerExecutor);
    publisherUnderConstruction.startPublisherUsing(buildDisruptor());
    return publisherUnderConstruction;
  }

  @SuppressWarnings("unchecked")
  private Disruptor<AvroEvent> buildDisruptor() {
    Disruptor<AvroEvent> disruptor = new Disruptor<>(avroEventFactory,
                                                     ringBufferSize,
                                                     consumerExecutor,
                                                     producerType,
                                                     waitStrategy);
    disruptor.handleExceptionsWith(new DisruptorExceptionHandler());
    disruptor.handleEventsWith(new AvroEventConsumer(avroFilename, avroSchema, rollingPolicy));
    return disruptor;
  }
}
