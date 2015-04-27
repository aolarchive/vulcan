package com.aol.advertising.dmp.disruptor.api.rolling;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import org.junit.Before;
import org.junit.Test;

public class DefaultRollingPolicyConfigurationTest {

  private int ROLLING_SIZE_IN_MB = 34;

  private DefaultRollingPolicyConfiguration defaultRollingPolicyConfigurationUnderTest;

  @Before
  public void setUp() {
    defaultRollingPolicyConfigurationUnderTest = new DefaultRollingPolicyConfiguration();
  }

  @Test(expected = IllegalArgumentException.class)
  public void whenFileRollingSizeSpecifiedIsLowerThan1Mb_thenThrowAnIllegalArgumentException() {
    defaultRollingPolicyConfigurationUnderTest.withFileRollingSizeOf(0);
  }

  @Test
  public void whenRollingSizeIsNotOverriden_thenAConfigurationWithARollingSizeOf50MbIsReturned() {
    assertThat(defaultRollingPolicyConfigurationUnderTest.getRollingSizeInMb(), is(equalTo(50)));
  }

  @Test
  public void whenArgumentRestrictionsAreSatisfied_thenAConfigurationWithSpecifiedRollingSizeIsReturned() {
    final DefaultRollingPolicyConfiguration configuration =
        defaultRollingPolicyConfigurationUnderTest.withFileRollingSizeOf(ROLLING_SIZE_IN_MB);

    assertThat(configuration.getRollingSizeInMb(), is(equalTo(ROLLING_SIZE_IN_MB)));
  }
}
