package com.aol.advertising.dmp.disruptor.api.builder.steps;

import java.io.File;

public interface AvroFilenameStep {

  /**
   * Configures the file writer to write to {@code avroFileName}.
   * 
   * @throws IllegalArgumentException if the file does not exist or does not have read/write
   *         permissions
   */
  AvroSchemaStep thatWritesTo(final File avroFileName) throws IllegalArgumentException;

}
