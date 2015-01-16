package com.aol.advertising.dmp.disruptor.api;

import java.nio.file.Path;

import org.apache.avro.Schema;

import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Factory API to create new instances of {@link DisruptorAvroFileWriter}. This API is suitable for
 * applications with dependency injection and declarative configuration styles such as Spring's
 * XML-based configuration.
 */
public class DisruptorAvroFileWriterFactory {

  private Path avroFileName;
  private Schema avroSchema;
  private int ringBufferSize;
  private ProducerType producerType;
  private WaitStrategy waitStrategy;
  private RollingPolicy rollingPolicy;
  
  public DisruptorAvroFileWriter createNewWriter() throws IllegalArgumentException {
    return DisruptorAvroFileWriterBuilder.createNewWriter().thatWritesTo(avroFileName)
                                                           .thatWritesRecordsOf(avroSchema)
                                                           .withRingBufferSize(ringBufferSize)
                                                           .withProducerType(producerType)
                                                           .withWaitStrategy(waitStrategy)
                                                           .withRollingPolicy(rollingPolicy)
                                                           .build();
  }


  public void setAvroFileName(final Path avroFileName) {
    this.avroFileName = avroFileName;
  }

  public void setAvroSchema(final Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public void setRingBufferSize(int ringBufferSize) {
    this.ringBufferSize = ringBufferSize;
  }

  public void setProducerType(final ProducerType producerType) {
    this.producerType = producerType;
  }

  public void setWaitStrategy(final WaitStrategy waitStrategy) {
    this.waitStrategy = waitStrategy;
  }

  public void setRollingPolicy(final RollingPolicy rollingPolicy) {
    this.rollingPolicy = rollingPolicy;
  }

}
