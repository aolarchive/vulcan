package com.aol.advertising.vulcan.rolling;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;

import com.aol.advertising.vulcan.ConfiguredUnitTest;
import com.aol.advertising.vulcan.rolling.SizeBasedRollingCondition;
import com.aol.advertising.vulcan.rolling.TimeAndSizeBasedRollingPolicyConfig;

public class SizeBasedRollingConditionTest extends ConfiguredUnitTest {

  private static final String AVRO_FILE_NAME = "Porrompompero";
  private static final int ONE_MB_IN_BYTES = 1_048_576;
  private static final int ROLLOVER_SIZE_IN_BYTES = ONE_MB_IN_BYTES;
  private static final int BYTES_TO_EVENT_RATIO = 1024;
  private static final TimeAndSizeBasedRollingPolicyConfig ROLLING_POLICY_CONFIGURATION =
      new TimeAndSizeBasedRollingPolicyConfig().withFileRollingSizeOf(ROLLOVER_SIZE_IN_BYTES / ONE_MB_IN_BYTES);

  private SizeBasedRollingCondition sizeBasedRollingConditionUnderTest;

  @Mock
  private SizeBasedRollingCondition sizeBasedRollingConditionMock;

  private File testAvroFile;
  private Path testAvroPath;
  private FileOutputStream testAvroFileStream;

  @Rule
  public TemporaryFolder testDirectory = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    resetTestFile();

    sizeBasedRollingConditionUnderTest = new SizeBasedRollingCondition(ROLLING_POLICY_CONFIGURATION.getRollingSizeInMb());
    sizeBasedRollingConditionUnderTest.registerAvroFileName(testAvroPath);
  }

  private void resetTestFile() throws Exception {
    if (testAvroFile != null && testAvroFile.exists()) {
      testAvroFile.delete();
    }
    testAvroFile = testDirectory.newFile(AVRO_FILE_NAME);
    testAvroPath = testAvroFile.toPath();
    testAvroFileStream = new FileOutputStream(testAvroFile);
  }

  @Test
  public void whenTheDestinationFileIsWrittenContinuously_thenTheExpectedNumberOfFileRollsAreDetectedByTheCondition() throws Exception {
    int TOTAL_NUMBER_OF_EVENTS_TO_WRITE = 3050;
    int EXPECTED_NUMBER_OF_ROLLS_DETECTED =
        (TOTAL_NUMBER_OF_EVENTS_TO_WRITE * BYTES_TO_EVENT_RATIO) / ROLLOVER_SIZE_IN_BYTES;
    byte[] event = Arrays.copyOf(new byte[0], BYTES_TO_EVENT_RATIO);

    int numberOfRollsDetectedByCondition = 0;
    for (int numberOfWrittenEventsSoFar = 0; numberOfWrittenEventsSoFar < TOTAL_NUMBER_OF_EVENTS_TO_WRITE; numberOfWrittenEventsSoFar++) {
      testAvroFileStream.write(event);
      testAvroFileStream.flush();

      if (sizeBasedRollingConditionUnderTest.shouldRollover()) {
        numberOfRollsDetectedByCondition++;
        sizeBasedRollingConditionUnderTest.signalRollover();
        resetTestFile();
      }
    }

    assertThat(numberOfRollsDetectedByCondition, is(equalTo(EXPECTED_NUMBER_OF_ROLLS_DETECTED)));
  }
}
