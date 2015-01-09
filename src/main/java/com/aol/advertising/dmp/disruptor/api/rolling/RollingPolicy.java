package com.aol.advertising.dmp.disruptor.api.rolling;

import java.io.File;

import org.apache.avro.specific.SpecificRecord;

/**
 * Callbacks used by the Avro writer to customize the rolling mechanism for the destination file
 * 
 */
public interface RollingPolicy {
  
  /**
   * Used to decide whether a rollover on the destination Avro file is due. Invoked every time
   * records are flushed to disk
   * 
   * @param avroFileName the destination Avro file
   * @param avroRecord the Avro record from the latest event consumed from the ring buffer
   */
  boolean shouldRollover(final File avroFileName, final SpecificRecord avroRecord);
  
  /**
   * Used to decide the name of the next file to be rolled
   * 
   * @return the name of the next rolled file
   */
  String getNextRolledFileName(final File avroFileName, int indexOfLastRolledFile);

}
