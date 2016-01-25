package com.aol.advertising.vulcan.api.builder.steps;

import org.apache.avro.Schema;

/**
 * @author Jaime Nuche
 *
 */
public interface AvroSchemaStep {

  /**
   * Configures the Avro writer to append {@code avroSchema} to any created destination files.
   * <p>
   * During startup, it is compared to the schema in the destination file (if file exists). If
   * schemas differ, the old file will be rolled and {@code avroSchema} used in the new one.
   * 
   * @throws IllegalArgumentException if the schema is null
   */
  OptionalSteps thatWritesRecordsOf(Schema avroSchema);

}
