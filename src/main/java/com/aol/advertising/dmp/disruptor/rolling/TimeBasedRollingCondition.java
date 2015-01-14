package com.aol.advertising.dmp.disruptor.rolling;

import org.joda.time.DateTime;

class TimeBasedRollingCondition {

  private DateTime lastRolloverDate;

  TimeBasedRollingCondition() {
    lastRolloverDate = UTCDateTime.now();
  }

  boolean rolloverShouldHappen() {
    return lastTimeRolloverHappenedBeforeToday();
  }

  private boolean lastTimeRolloverHappenedBeforeToday() {
    return lastRolloverDate.isBefore(UTCDateTime.beginningOfToday());
  }

}
