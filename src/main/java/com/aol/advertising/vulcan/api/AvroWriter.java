package com.aol.advertising.vulcan.api;

import org.apache.avro.specific.SpecificRecord;

/**
 * Disruptor-based Avro writer. Consists of a <a
 * href="http://lmax-exchange.github.io/disruptor/">disruptor</a> buffer with a backend consumer
 * that writes Avro records to a file in the local filesystem.
 *
 * @see AvroWriterBuilder
 * @see AvroWriterFactory
 *
 * @author Jaime Nuche
 *
 */
public interface AvroWriter extends AutoCloseable {

  /**
   * Writes an Avro record to file
   */
  void write(SpecificRecord avroRecord);

}
