package com.aol.advertising.dmp.disruptor.api.builder;

import com.aol.advertising.dmp.disruptor.DisruptorAvroFileWriter;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

public interface OptionalSteps {

  OptionalSteps withARingBufferOfSize(int ringBufferSize);

  OptionalSteps withADisruptorSuitableForAProducerOfType(final ProducerType producerType);

  OptionalSteps withAnAvroWriterThatWaitsForNewEventsWithStrategy(final WaitStrategy waitStrategy);

  DisruptorAvroFileWriter build();

}
