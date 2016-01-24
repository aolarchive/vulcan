package com.aol.advertising.vulcan.api.rolling;

import java.nio.file.Path;

import org.apache.avro.specific.SpecificRecord;

import com.aol.advertising.vulcan.exception.FileRollingException;

/**
 * Customizable rolling mechanism for the target Avro file
 * 
 * @author Jaime Nuche
 *
 */
public interface RollingPolicy {

  /**
   * Register with the policy the file that is subject to rolling. Initializes policy
   * 
   * @param avroFilename the target Avro file
   */
  void registerAvroFilename(Path avroFilename);

  /**
   * Decide if a rollover on the target Avro file is due
   * 
   * @param avroRecord the Avro record from the latest event consumed from the ring buffer
   */
  boolean shouldRollover(SpecificRecord avroRecord);

  /**
   * Roll over the target Avro file using the policy's naming convention. File is rolled under the same directory
   * 
   * @throws {@code FileRollingException} if the file could not be rolled
   */
  void rolloverAvroFile() throws FileRollingException;
}
