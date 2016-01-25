package com.aol.advertising.vulcan.api.builder.steps;

import java.nio.file.Path;

public interface AvroFilenameStep {

  /**
   * Configures the file writer to write to {@code avroFilename}.
   * 
   * @throws IllegalArgumentException if it is not possible to read and write to the specified
   *         destination
   */
  AvroSchemaStep thatWritesTo(Path avroFilename);

  /**
   * Configures the file writer to write to {@code avroFilename}.
   * 
   * @throws IllegalArgumentException if it is not possible to read and write to the specified
   *         destination
   */
  AvroSchemaStep thatWritesTo(String avroFilename);

}
