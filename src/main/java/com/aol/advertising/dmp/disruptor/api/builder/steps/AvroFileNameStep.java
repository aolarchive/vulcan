package com.aol.advertising.dmp.disruptor.api.builder.steps;

import java.io.File;

public interface AvroFileNameStep {

  /**
   * Configures the file writer to write to {@code avroFileName}.
   * 
   * @throws IllegalArgumentException if the file does not have read/write permissions or exists but
   *         it's a directory
   */
  AvroSchemaStep thatWritesTo(final File avroFileName) throws IllegalArgumentException;

}
