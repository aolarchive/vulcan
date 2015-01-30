package com.aol.advertising.dmp.disruptor.rolling;

import org.joda.time.DateTime;

class TimeBasedRollingCondition {

  private DateTime lastRolloverDate;

  TimeBasedRollingCondition() {
    lastRolloverDate = DateTime.now();
  }

  boolean lastRolloverHappenedBeforeToday() {
    return lastRolloverDate.isBefore(new DateTime().withTimeAtStartOfDay());
  }

  void signalRollover() {
    lastRolloverDate = DateTime.now();
  }

}
