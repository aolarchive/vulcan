package com.aol.advertising.dmp.disruptor.rolling;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Files.class, SizeBasedRollingCondition.class})
public class SizeBasedRollingConditionTest {

  private static final int ONE_MB_IN_BYTES = 1_048_576;
  private static final int ROLLOVER_SIZE_IN_BYTES = ONE_MB_IN_BYTES;
  private static final int BYTES_TO_EVENT_RATIO = 1024;

  private SizeBasedRollingCondition sizeBasedRollingConditionUnderTest;

  @Mock
  private Path avroFileNameMock;
  @Mock
  private SizeBasedRollingCondition sizeBasedRollingConditionMock;

  @Before
  public void setUp() throws Exception {
    initMocks();
    sizeBasedRollingConditionUnderTest = new SizeBasedRollingCondition(avroFileNameMock, ROLLOVER_SIZE_IN_BYTES / ONE_MB_IN_BYTES);
  }

  private void initMocks() throws Exception {
    MockitoAnnotations.initMocks(this);
    mockStatic(Files.class);
  }
  
  @Test
  public void whenTheDestinationFileIsWrittenContinuously_thenTheExpectedNumberOfFileRollsAreDetectedByTheCondition() throws Exception {
    final int TOTAL_NUMBER_OF_EVENTS_TO_WRITE = 3050;
    final int NUMBER_OF_EVENTS_PER_ROLLED_FILE = ROLLOVER_SIZE_IN_BYTES / BYTES_TO_EVENT_RATIO;
    final int EXPECTED_NUMBER_OF_ROLLS_DETECTED = (TOTAL_NUMBER_OF_EVENTS_TO_WRITE * BYTES_TO_EVENT_RATIO) / ROLLOVER_SIZE_IN_BYTES;
    
    int numberOfRollsDetectedByCondition = 0;
    for (int numberOfWrittenEventsSoFar = 0; numberOfWrittenEventsSoFar < TOTAL_NUMBER_OF_EVENTS_TO_WRITE; numberOfWrittenEventsSoFar++) {
      final int eventsInRolledFiles = NUMBER_OF_EVENTS_PER_ROLLED_FILE * numberOfRollsDetectedByCondition;
      when(Files.size(avroFileNameMock)).thenReturn((long) (numberOfWrittenEventsSoFar - eventsInRolledFiles) * BYTES_TO_EVENT_RATIO);
      
      if (sizeBasedRollingConditionUnderTest.sizeThresholdHasBeenHit()) {
        numberOfRollsDetectedByCondition++;
        sizeBasedRollingConditionUnderTest.signalRollover();
      }
    }

    assertThat(numberOfRollsDetectedByCondition, is(equalTo(EXPECTED_NUMBER_OF_ROLLS_DETECTED)));
  }

}
