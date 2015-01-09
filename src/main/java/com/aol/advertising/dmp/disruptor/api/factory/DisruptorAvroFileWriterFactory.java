package com.aol.advertising.dmp.disruptor.api.factory;

import java.io.File;

import org.apache.avro.Schema;

import com.aol.advertising.dmp.disruptor.api.DisruptorAvroFileWriter;
import com.aol.advertising.dmp.disruptor.api.builder.DisruptorAvroFileWriterBuilder;
import com.aol.advertising.dmp.disruptor.api.rolling.RollingPolicy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Factory API to create new instances of {@link DisruptorAvroFileWriter}. This API is suitable for
 * applications with dependency injection and declarative configuration styles such as Spring's
 * XML-based configuration.
 */
public class DisruptorAvroFileWriterFactory {

  private File avroFileName;
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


  public void setAvroFileName(File avroFileName) {
    this.avroFileName = avroFileName;
  }

  public void setAvroSchema(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public void setRingBufferSize(int ringBufferSize) {
    this.ringBufferSize = ringBufferSize;
  }

  public void setProducerType(ProducerType producerType) {
    this.producerType = producerType;
  }

  public void setWaitStrategy(WaitStrategy waitStrategy) {
    this.waitStrategy = waitStrategy;
  }

  public void setRollingPolicy(RollingPolicy rollingPolicy) {
    this.rollingPolicy = rollingPolicy;
  }

}
