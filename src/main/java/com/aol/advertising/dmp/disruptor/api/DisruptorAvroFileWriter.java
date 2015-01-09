package com.aol.advertising.dmp.disruptor.api;

import org.apache.avro.specific.SpecificRecord;

import com.aol.advertising.dmp.disruptor.api.builder.DisruptorAvroFileWriterBuilder;
import com.aol.advertising.dmp.disruptor.api.factory.DisruptorAvroFileWriterFactory;

/**
 * Disruptor-based Avro writer. Consists of a <a
 * href="http://lmax-exchange.github.io/disruptor/">disruptor</a> buffer with a backend consumer
 * that writes Avro records to a file in the local filesystem.
 * 
 * @see DisruptorAvroFileWriterBuilder
 * @see DisruptorAvroFileWriterFactory
 * 
 */
public interface DisruptorAvroFileWriter extends AutoCloseable {

  /**
   * Writes an Avro record to file
   */
  void write(final SpecificRecord avroRecord);

}
