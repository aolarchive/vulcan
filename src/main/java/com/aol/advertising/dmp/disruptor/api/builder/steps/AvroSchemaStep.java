package com.aol.advertising.dmp.disruptor.api.builder.steps;

import org.apache.avro.Schema;

public interface AvroSchemaStep {

  /**
   * Configures the Avro writer to compare {@code avroSchema} to the schema in the destination file
   * (if any). If schemas differ, the old file will be rolled and {@code avroSchema} used in the new
   * file.
   * 
   * @throws IllegalArgumentException if the schema is null
   */
  OptionalSteps thatWritesRecordsWith(final Schema avroSchema) throws IllegalArgumentException;

}
