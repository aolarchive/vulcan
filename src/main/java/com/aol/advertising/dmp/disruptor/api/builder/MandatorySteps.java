package com.aol.advertising.dmp.disruptor.api.builder;

import java.io.File;

public interface MandatorySteps {

  /**
   * Configures the file writer to write to {@code avroFilename}.
   * 
   * @throws IllegalArgumentException if the file does not exist or does not have read/write
   *         permissions
   */
  OptionalSteps thatWritesTo(final File avroFilename) throws IllegalArgumentException;

}