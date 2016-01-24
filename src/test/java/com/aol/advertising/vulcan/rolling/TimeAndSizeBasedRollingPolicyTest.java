package com.aol.advertising.vulcan.rolling;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;

import com.aol.advertising.vulcan.disruptor.ConfiguredUnitTest;
import com.aol.advertising.vulcan.exception.FileRollingException;
import com.aol.advertising.vulcan.rolling.SizeBasedRollingCondition;
import com.aol.advertising.vulcan.rolling.TimeAndSizeBasedRollingPolicy;
import com.aol.advertising.vulcan.rolling.TimeAndSizeBasedRollingPolicyConfig;
import com.aol.advertising.vulcan.rolling.TimeBasedRollingCondition;

public class TimeAndSizeBasedRollingPolicyTest extends ConfiguredUnitTest {

  private static final String AVRO_FILE_NAME = "MonsterTruckMadness";
  private static final int ROLLOVER_SIZE = 42;
  private static final TimeAndSizeBasedRollingPolicyConfig ROLLING_POLICY_CONFIGURATION =
      new TimeAndSizeBasedRollingPolicyConfig().withFileRollingSizeOf(ROLLOVER_SIZE);

  private TimeAndSizeBasedRollingPolicy timeAndSizeBasedRollingPolicyUnderTest;

  @Mock
  private TimeBasedRollingCondition timeBasedRollingConditionMock;
  @Mock
  private SizeBasedRollingCondition sizeBasedRollingConditionMock;

  private File testAvroFile;
  private Path testAvroPath;

  @Rule
  public TemporaryFolder testDirectory = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    initTestData();

    timeAndSizeBasedRollingPolicyUnderTest = new TimeAndSizeBasedRollingPolicy(ROLLING_POLICY_CONFIGURATION);

    injectMocks();
  }

  private void initTestData() throws IOException {
    initTestAvroFileTo(testDirectory.newFile(AVRO_FILE_NAME));
  }

  private void initTestAvroFileTo(File avroFile) {
    testAvroFile = avroFile;
    testAvroPath = testAvroFile.toPath();
  }

  private void injectMocks() {
    Whitebox.setInternalState(timeAndSizeBasedRollingPolicyUnderTest, "lastRolloverHappenedBeforeToday", timeBasedRollingConditionMock);
    Whitebox.setInternalState(timeAndSizeBasedRollingPolicyUnderTest, "sizeThresholdHasBeenHit", sizeBasedRollingConditionMock);
  }

  @Test
  public void whenThePolicyIsInitialized_thenTheAvroFilenameIsRegisteredWithTheSizeBaseRollingCondition() throws Exception {
    whenThePolicyIsInitialized();

    verify(sizeBasedRollingConditionMock).registerAvroFileName(testAvroPath);
  }

  @Test
  public void whenThePolicyIsInitialized_thenTheRollingIndexContinuesWhereWeLeftOff() throws Exception {
    givenTheMaximumIndexInTheDirIs20();

    whenThePolicyIsInitialized();
    timeAndSizeBasedRollingPolicyUnderTest.rolloverAvroFile();

    thenTheIndexOfTheLastRolledFileIs21();
  }

  @Test
  public void shouldRolloverDecisionIsDelegatedToConditions() {
    givenThePolicyIsInitialized();

    timeAndSizeBasedRollingPolicyUnderTest.shouldRollover(null);

    thenDecisionIsDelegatedToConditions();
  }

  @Test
  public void whenFileIsRolledOver_thenTheRolledFileNameFollowsTheExpectedPattern() throws Exception {
    givenThePolicyIsInitialized();

    timeAndSizeBasedRollingPolicyUnderTest.rolloverAvroFile();

    thenTheRolledFileNameFollowsTheExpectedPattern();
  }

  @Test
  public void whenFileIsRolledOver_andTimeBasedRollIsDue_thenRolledFileNameDateIsYesterday() throws Exception {
    givenThePolicyIsInitialized();
    givenTimeBasedRollIsDue();

    timeAndSizeBasedRollingPolicyUnderTest.rolloverAvroFile();

    DateTime date = getDateTimeFrom(getLastRolledfile());
    assertThat(date, is(equalTo(DateTime.now().minusDays(1).withTimeAtStartOfDay())));
  }

  @Test
  public void whenFileIsRolledOver_andTimeBasedRollIsNotDue_thenRolledFileNameDateIsToday() throws Exception {
    givenThePolicyIsInitialized();

    timeAndSizeBasedRollingPolicyUnderTest.rolloverAvroFile();

    DateTime date = getDateTimeFrom(getLastRolledfile());
    assertThat(date, is(equalTo(DateTime.now().withTimeAtStartOfDay())));
  }

  @Test
  public void whenFileIsRolledOver_andTimeBasedRollIsDue_thenTheIndexOfTheRolledFileNameIsResetToZeroInTheNextCall()
      throws Exception {
    givenThePolicyIsInitialized();
    givenTimeBasedRollIsDue();
    givenFiledIsRolled();
    givenTimeBasedRollIsNotDue();

    timeAndSizeBasedRollingPolicyUnderTest.rolloverAvroFile();

    int index = getIndexFrom(getLastRolledfile());
    assertThat(index, is(equalTo(0)));
  }

  @Test
  public void whenFileIsRolledOver_andTimeBasedRollIsNotDue_thenTheIndexOfTheRolledFileNameIsIncreasedByOne()
      throws Exception {
    givenThePolicyIsInitialized();
    givenTimeBasedRollIsNotDue();
    givenFiledIsRolled();
    int initialIndex = getIndexFrom(getLastRolledfile());

    timeAndSizeBasedRollingPolicyUnderTest.rolloverAvroFile();

    int finalIndex = getIndexFrom(getLastRolledfile());
    assertThat(finalIndex, is(equalTo(initialIndex + 1)));
  }

  @Test
  public void whenFileIsRolledOver_thenInfoAboutARolloverEventIsPropagatedToConditions() throws Exception {
    givenThePolicyIsInitialized();

    timeAndSizeBasedRollingPolicyUnderTest.rolloverAvroFile();

    thenInfoAboutTheEventIsPropagatedToConditions();
  }

  @Test(expected = FileRollingException.class)
  public void whenFileIsRolledOver_andTheFileFailsToRoll_thenAFileRollingExceptionIsThrown() throws Exception {
    givenThePolicyIsInitialized();
    testDirectory.getRoot().setWritable(false);

    timeAndSizeBasedRollingPolicyUnderTest.rolloverAvroFile();
  }

  private void givenTheMaximumIndexInTheDirIs20() throws Exception {
    testDirectory.newFile(AVRO_FILE_NAME + "-04.20.log");
    testDirectory.newFile((AVRO_FILE_NAME + "-02.16.log"));
  }

  private void givenThePolicyIsInitialized() {
    timeAndSizeBasedRollingPolicyUnderTest.registerAvroFilename(testAvroPath);
  }

  private void givenTimeBasedRollIsDue() {
    when(timeBasedRollingConditionMock.shouldRollover()).thenReturn(true);
  }

  private void givenFiledIsRolled() throws Exception {
    timeAndSizeBasedRollingPolicyUnderTest.rolloverAvroFile();
    initTestAvroFileTo(testDirectory.newFile(AVRO_FILE_NAME));
  }

  private void givenTimeBasedRollIsNotDue() {
    when(timeBasedRollingConditionMock.shouldRollover()).thenReturn(false);
  }

  private void whenThePolicyIsInitialized() {
    givenThePolicyIsInitialized();
  }

  private void thenTheIndexOfTheLastRolledFileIs21() throws Exception {
    assertThat(getIndexFrom(getLastRolledfile()), is(equalTo(21)));
  }

  private void thenDecisionIsDelegatedToConditions() {
    verify(timeBasedRollingConditionMock).shouldRollover();
    verify(sizeBasedRollingConditionMock).shouldRollover();
  }

  private void thenTheRolledFileNameFollowsTheExpectedPattern() throws Exception {
    assertThat(getLastRolledfile().getFileName(), matchesRegex("[a-zA-Z/]+-\\d{4}-\\d{2}-\\d{2}\\.\\d+\\.log"));
  }

  private void thenInfoAboutTheEventIsPropagatedToConditions() {
    verify(timeBasedRollingConditionMock).signalRollover();
    verify(sizeBasedRollingConditionMock).signalRollover();
  }

  private Path getLastRolledfile() throws Exception {
    List<Path> rolledFilesInTestDir = new ArrayList<>();
    for (Path rolledFile : Files.newDirectoryStream(testDirectory.getRoot().toPath(), new RolledFilePattern())) {
      rolledFilesInTestDir.add(rolledFile);
    }
    Collections.sort(rolledFilesInTestDir);
    return rolledFilesInTestDir.get(rolledFilesInTestDir.size() - 1);
  }

  private static Matcher<Path> matchesRegex(final String regex) {
    return new TypeSafeMatcher<Path>() {

      @Override
      public void describeTo(Description desc) {
        desc.appendText("Should match regex \"" + regex + "\"");
      }

      @Override
      protected boolean matchesSafely(Path pathToMatch) {
        return pathToMatch.toString().matches(regex);
      }

    };
  }

  private DateTime getDateTimeFrom(Path fileName) {
    java.util.regex.Matcher dateTimeMatcher =
        Pattern.compile(testAvroPath.getFileName().toString() + ".+(\\d{4}-\\d{2}-\\d{2})\\.\\d+\\.log$")
               .matcher(fileName.getFileName().toString());
    return dateTimeMatcher.find() ? DateTime.parse(dateTimeMatcher.group(1)) : new DateTime(0);
  }

  private int getIndexFrom(Path fileName) {
    java.util.regex.Matcher fileIndexMatcher =
        Pattern.compile(testAvroPath.getFileName().toString() + ".+\\d{2}\\.(\\d+).log$")
               .matcher(fileName.getFileName().toString());
    return fileIndexMatcher.find() ? Integer.parseInt(fileIndexMatcher.group(1)) : Integer.MIN_VALUE;
  }

  private static class RolledFilePattern implements Filter<Path> {
    @Override
    public boolean accept(Path fileInTestDir) throws IOException {
      return fileInTestDir.toString().matches(".+.\\d+\\.log$");
    }
  }
}
