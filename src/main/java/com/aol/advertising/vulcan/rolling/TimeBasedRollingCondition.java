package com.aol.advertising.vulcan.rolling;

import org.joda.time.DateTime;

class TimeBasedRollingCondition implements RollingCondition {

  private DateTime lastRolloverDate;

  TimeBasedRollingCondition() {
    lastRolloverDate = DateTime.now();
  }

  @Override
  public boolean shouldRollover() {
    return lastRolloverDate.isBefore(new DateTime().withTimeAtStartOfDay());
  }

  @Override
  public void signalRollover() {
    lastRolloverDate = DateTime.now();
  }
}
