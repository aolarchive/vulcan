package com.aol.advertising.dmp.disruptor.api.factory;

import java.io.File;

import org.apache.avro.Schema;

import com.aol.advertising.dmp.disruptor.DisruptorAvroFileWriter;
import com.aol.advertising.dmp.disruptor.api.builder.DisruptorAvroFileWriterBuilder;
import com.aol.advertising.dmp.disruptor.rolling.RollingPolicy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Factory API to create new instances of {@link DisruptorAvroFileWriter}. This API is suitable for
 * applications with dependency injection and declarative configuration styles such as Spring's
 * XML-based configuration.
 */
public class DisruptorAvroFileWriterFactory {

  public static DisruptorAvroFileWriter createNewWriterWithDefaults(final File avroFileName,
                                                                    final Schema avroSchema) throws IllegalArgumentException {
    return DisruptorAvroFileWriterBuilder.createNewWriter().thatWritesTo(avroFileName)
                                                           .thatWritesRecordsWith(avroSchema)
                                                           .build();
  }
  
  public static DisruptorAvroFileWriter createNewWriter(final File avroFileName,
                                                        final Schema avroSchema,
                                                        int ringBufferSize,
                                                        final ProducerType producerType,
                                                        final WaitStrategy waitStrategy,
                                                        final RollingPolicy rollingPolicy) throws IllegalArgumentException {
    return DisruptorAvroFileWriterBuilder.createNewWriter().thatWritesTo(avroFileName)
                                                           .thatWritesRecordsWith(avroSchema)
                                                           .withARingBufferOfSize(ringBufferSize)
                                                           .withAProducerOfType(producerType)
                                                           .withWaitStrategy(waitStrategy)
                                                           .withRollingPolicy(rollingPolicy)
                                                           .build();
  }

}
