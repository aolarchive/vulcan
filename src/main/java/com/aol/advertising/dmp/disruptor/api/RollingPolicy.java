package com.aol.advertising.dmp.disruptor.api;

import java.nio.file.Path;

import org.apache.avro.specific.SpecificRecord;

/**
 * Callbacks used by the Avro writer to customize the rolling mechanism for the destination file
 * 
 */
public interface RollingPolicy {
  
  /**
   * Used to decide whether a rollover on the destination Avro file is due
   * 
   * @param avroFileName the destination Avro file
   * @param avroRecord the Avro record from the latest event consumed from the ring buffer
   */
  boolean shouldRollover(final Path avroFileName, final SpecificRecord avroRecord);
  
  /**
   * Used to decide the name of the next file to be rolled
   * 
   * @param avroFileName the destination Avro file
   * @return the name of the next rolled file
   */
  Path getNextRolledFileName(final Path avroFileName);

  /**
   * Used to signal rolling occurrences to the policy object
   * 
   * @param avroFileName the destination Avro file
   */
  void signalRolloverOf(final Path avroFileName);

}
