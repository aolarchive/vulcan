package com.aol.advertising.dmp.disruptor.rolling;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import org.junit.Before;
import org.junit.Test;

import com.aol.advertising.dmp.disruptor.rolling.TimeAndSizeBasedRollingPolicyConfig;

public class TimeAndSizeBasedRollingPolicyConfigTest {

  private int ROLLING_SIZE_IN_MB = 34;

  private TimeAndSizeBasedRollingPolicyConfig timeAndSizeBasedRollingPolicyConfigUnderTest;

  @Before
  public void setUp() {
    timeAndSizeBasedRollingPolicyConfigUnderTest = new TimeAndSizeBasedRollingPolicyConfig();
  }

  @Test(expected = IllegalArgumentException.class)
  public void whenFileRollingSizeSpecifiedIsLowerThan1Mb_thenThrowAnIllegalArgumentException() {
    timeAndSizeBasedRollingPolicyConfigUnderTest.withFileRollingSizeOf(0);
  }

  @Test
  public void whenRollingSizeIsNotOverriden_thenAConfigurationWithARollingSizeOf50MbIsReturned() {
    assertThat(timeAndSizeBasedRollingPolicyConfigUnderTest.getRollingSizeInMb(), is(equalTo(50)));
  }

  @Test
  public void whenArgumentRestrictionsAreSatisfied_thenAConfigurationWithSpecifiedRollingSizeIsReturned() {
    final TimeAndSizeBasedRollingPolicyConfig configuration =
        timeAndSizeBasedRollingPolicyConfigUnderTest.withFileRollingSizeOf(ROLLING_SIZE_IN_MB);

    assertThat(configuration.getRollingSizeInMb(), is(equalTo(ROLLING_SIZE_IN_MB)));
  }
}
