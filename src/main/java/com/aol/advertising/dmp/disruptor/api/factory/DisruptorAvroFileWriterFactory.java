package com.aol.advertising.dmp.disruptor.api.factory;

import java.io.File;

import com.aol.advertising.dmp.disruptor.DisruptorAvroFileWriter;
import com.aol.advertising.dmp.disruptor.api.builder.DisruptorAvroFileWriterBuilder;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Factory API to create new instances of {@link DisruptorAvroFileWriter}. This API is suitable for
 * applications with dependency injection and declarative configuration styles such as Spring's
 * XML-based configuration.
 */
public class DisruptorAvroFileWriterFactory {

  public static DisruptorAvroFileWriter createNewWriterWithDefaults(final File avroFilename) throws IllegalArgumentException {
    return DisruptorAvroFileWriterBuilder.createNewWriter().thatWritesTo(avroFilename).build();
  }
  
  public static DisruptorAvroFileWriter createNewWriter(final File avroFilename,
                                                        int ringBufferSize,
                                                        final ProducerType producerType,
                                                        final WaitStrategy waitStrategy) throws IllegalArgumentException {
    return DisruptorAvroFileWriterBuilder.createNewWriter().thatWritesTo(avroFilename)
                                                           .withARingBufferOfSize(ringBufferSize)
                                                           .withAProducerOfType(producerType)
                                                           .withWaitStrategy(waitStrategy)
                                                           .build();
  }

}
