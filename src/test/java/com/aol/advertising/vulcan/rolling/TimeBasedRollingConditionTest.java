package com.aol.advertising.vulcan.rolling;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.aol.advertising.vulcan.disruptor.ConfiguredUnitTest;
import com.aol.advertising.vulcan.rolling.TimeBasedRollingCondition;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DateTime.class, TimeBasedRollingCondition.class})
public class TimeBasedRollingConditionTest extends ConfiguredUnitTest {

  private TimeBasedRollingCondition timeBasedRollingConditionUnderTest;

  @Mock
  private DateTime lastRollDateTimeMock;
  @Mock
  private DateTime nowMock;
  @Mock
  private DateTime startOfTodayMock;

  @Before
  public void setUp() throws Exception {
    initMocks();

    timeBasedRollingConditionUnderTest = new TimeBasedRollingCondition();
  }

  private void initMocks() throws Exception {
    mockStatic(DateTime.class);

    when(DateTime.now()).thenReturn(lastRollDateTimeMock, nowMock);
    whenNew(DateTime.class).withNoArguments().thenReturn(nowMock);
  }

  @Test
  public void whenARolloverIsSignaled_thenLastRollIsUpdatedToNow() {
    timeBasedRollingConditionUnderTest.signalRollover();

    assertThat(Whitebox.getInternalState(timeBasedRollingConditionUnderTest, DateTime.class), is(equalTo(nowMock)));
  }

  @Test
  public void whenLastRollHappenedBeforeToday_thenRolloverShouldHappen() {
    givenLastRollHappenedBeforeToday();

    boolean rolloverShouldHappen = timeBasedRollingConditionUnderTest.shouldRollover();

    assertThat(rolloverShouldHappen, is(equalTo(true)));
  }

  private void givenLastRollHappenedBeforeToday() {
    when(nowMock.withTimeAtStartOfDay()).thenReturn(startOfTodayMock);
    when(lastRollDateTimeMock.isBefore(startOfTodayMock)).thenReturn(true);
  }

}
