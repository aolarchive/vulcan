package com.aol.advertising.dmp.disruptor.api.builder.steps;

import java.nio.file.Path;

public interface AvroFileNameStep {

  /**
   * Configures the file writer to write to {@code avroFileName}.
   * 
   * @throws IllegalArgumentException if it is not possible to read and write to the specified
   *         destination
   */
  AvroSchemaStep thatWritesTo(final Path avroFileName) throws IllegalArgumentException;

}
