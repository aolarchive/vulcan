package com.aol.advertising.vulcan.api;

import org.apache.avro.Schema;

import com.aol.advertising.vulcan.api.builder.steps.OptionalSteps;
import com.aol.advertising.vulcan.api.rolling.RollingPolicy;
import com.aol.advertising.vulcan.rolling.TimeAndSizeBasedRollingPolicyConfig;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Factory API to create new instances of {@link AvroWriter}. This API is suitable for applications
 * with dependency injection and declarative configuration styles such as Spring's XML-based
 * configuration.
 * 
 * @author Jaime Nuche
 *
 */
public class AvroWriterFactory {

  private String avroFilename;
  private Schema avroSchema;
  private int ringBufferSize;
  private ProducerType producerType;
  private WaitStrategy waitStrategy;
  private RollingPolicy rollingPolicy;
  private TimeAndSizeBasedRollingPolicyConfig defaultRollingPolicyConfiguration;
  
  public AvroWriter createNewWriter() {
    OptionalSteps writerBuilder = AvroWriterBuilder.startCreatingANewWriter()
                                                   .thatWritesTo(avroFilename)
                                                   .thatWritesRecordsOf(avroSchema);
    if (ringBufferSize > 0) {
      writerBuilder.withRingBufferSize(ringBufferSize);
    }
    if (producerType != null) {
      writerBuilder.withProducerType(producerType);
    }
    if (waitStrategy != null) {
      writerBuilder.withWaitStrategy(waitStrategy);
    }
    if (defaultRollingPolicyConfiguration != null) {
      writerBuilder.withDefaultRollingPolicyConfiguration(defaultRollingPolicyConfiguration);
    }
    if (rollingPolicy != null) {
      writerBuilder.withRollingPolicy(rollingPolicy);
    }
    return writerBuilder.createNewWriter();
  }


  public void setAvroFilename(String avroFilename) {
    this.avroFilename = avroFilename;
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

  public void setDefaultRollingPolicyConfiguration(TimeAndSizeBasedRollingPolicyConfig configuration) {
    this.defaultRollingPolicyConfiguration = configuration;
  }

  public void setRollingPolicy(RollingPolicy rollingPolicy) {
    this.rollingPolicy = rollingPolicy;
  }
}
