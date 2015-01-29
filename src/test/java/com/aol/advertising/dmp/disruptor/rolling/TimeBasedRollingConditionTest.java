package com.aol.advertising.dmp.disruptor.rolling;

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
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DateTime.class, TimeBasedRollingCondition.class})
public class TimeBasedRollingConditionTest {

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
    MockitoAnnotations.initMocks(this);
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

    final boolean rolloverShouldHappen = timeBasedRollingConditionUnderTest.lastRolloverHappenedBeforeToday();

    assertThat(rolloverShouldHappen, is(equalTo(true)));
  }

  private void givenLastRollHappenedBeforeToday() {
    when(nowMock.withTimeAtStartOfDay()).thenReturn(startOfTodayMock);
    when(lastRollDateTimeMock.isBefore(startOfTodayMock)).thenReturn(true);
  }

}
