package com.aol.advertising.dmp.disruptor.api.factory;

import java.io.File;

import com.aol.advertising.dmp.disruptor.DisruptorAvroFileWriter;
import com.aol.advertising.dmp.disruptor.api.builder.DisruptorAvroFileWriterBuilder;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Factory API
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
                                                           .withADisruptorSuitableForAProducerOfType(producerType)
                                                           .withAnAvroWriterThatWaitsForNewEventsWithStrategy(waitStrategy)
                                                           .build();
  }

}
