package com.aol.advertising.dmp.disruptor.rolling;

import java.io.File;

import org.apache.avro.specific.SpecificRecord;

import com.aol.advertising.dmp.disruptor.api.RollingPolicy;

public class TimeAndSizeBasedRollingPolicy implements RollingPolicy {

  private final int rolloverTriggeringSizeInMB;

  public TimeAndSizeBasedRollingPolicy(int rolloverTriggeringSizeInMB) {
    this.rolloverTriggeringSizeInMB = rolloverTriggeringSizeInMB;
  }

  @Override
  public boolean shouldRollover(final File avroFileName, final SpecificRecord avroRecord) {
    return false;
  }

  @Override
  public String getNextRolledFileName(final File avroFileName) {
    return "blah";
  }

}
