package com.aol.advertising.dmp.disruptor.api.builder;

import com.aol.advertising.dmp.disruptor.DisruptorAvroFileWriter;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

public interface OptionalSteps {

  /**
   * Number of ring buffer events/entries to be pre-allocated by the disruptor. Note that the final
   * memory usage depends on the actual size of the Avro entries used. <b>This number should be a
   * power of 2</b>
   * <p>
   * Default is 1024 entries
   */
  OptionalSteps withARingBufferOfSize(int ringBufferSize);

  /**
   * Configures a disruptor suitable for a producer of type {@code producerType}. Note that a
   * disruptor configured with {@link ProducerType#SINGLE} is more performant but not safe in a
   * environment with multiple producers.
   * <p>
   * Default is {@link ProducerType#MULTI}
   */
  OptionalSteps withAProducerOfType(final ProducerType producerType);

  /**
   * Configures the Avro writer with the waiting strategy {@code waitStrategy}. The waiting strategy
   * is used to consume new events from the ring buffer. Typically, they represent varying degrees
   * of compromise between optimal latency and throughput, and CPU usage.
   * <p>
   * Default is {@link SleepingWaitStrategy}
   */
  OptionalSteps withWaitStrategy(final WaitStrategy waitStrategy);

  /**
   * Finish configuration and create a new {@link DisruptorAvroFileWriter} object
   */
  DisruptorAvroFileWriter build();

}
