package com.aol.advertising.dmp.disruptor.api.rolling;

import java.nio.file.Path;

import org.apache.avro.specific.SpecificRecord;

/**
 * Callbacks used by the Avro writer to customize the rolling mechanism for the destination file
 * 
 */
public interface RollingPolicy {
  
  /**
   * Used to register with the policy the file that is subject to rolling
   * 
   * @param avroFileName the destination Avro file
   */
  void registerAvroFileName(final Path avroFileName);

  /**
   * Used to decide whether a rollover on the destination Avro file is due
   * 
   * @param avroRecord the Avro record from the latest event consumed from the ring buffer
   */
  boolean shouldRollover(final SpecificRecord avroRecord);
  
  /**
   * Used to decide the name of the next rolled file name. The policy can assume the file will be
   * rolled between calls to this method
   * 
   * @return the name of the next rolled file
   */
  Path getNextRolledFileName();

}
