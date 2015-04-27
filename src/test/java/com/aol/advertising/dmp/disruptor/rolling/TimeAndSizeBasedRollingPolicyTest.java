package com.aol.advertising.dmp.disruptor.rolling;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.aol.advertising.dmp.disruptor.ConfiguredUnitTest;
import com.aol.advertising.dmp.disruptor.api.rolling.DefaultRollingPolicyConfiguration;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Files.class, TimeBasedRollingCondition.class, SizeBasedRollingCondition.class, TimeAndSizeBasedRollingPolicy.class})
public class TimeAndSizeBasedRollingPolicyTest extends ConfiguredUnitTest {

  private static final String AVRO_FILE_NAME = "MonsterTruckMadness";
  private static final int ROLLOVER_SIZE = 42;
  private static final DefaultRollingPolicyConfiguration DEFAULT_ROLLING_POLICY_CONFIGURATION =
      new DefaultRollingPolicyConfiguration().withFileRollingSizeOf(ROLLOVER_SIZE);

  private TimeAndSizeBasedRollingPolicy timeAndSizeBasedRollingPolicyUnderTest;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Path avroFileNameMock;
  @Mock
  private Path parentDirMock;
  @Mock
  private DirectoryStream<Path> dirContentsMock;
  @Mock
  private Iterator<Path> dirContentsIteratorMock;
  @Mock
  private TimeBasedRollingCondition timeBasedRollingConditionMock;
  @Mock
  private SizeBasedRollingCondition sizeBasedRollingConditionMock;

  @Before
  public void setUp() throws Exception {
    initMocks();
    wireUpMocks();
  }

  private void initMocks() throws Exception {
    mockStatic(Files.class);
    mockStatic(TimeBasedRollingCondition.class);
    mockStatic(SizeBasedRollingCondition.class);
    
    whenNew(TimeBasedRollingCondition.class).withNoArguments().thenReturn(timeBasedRollingConditionMock);
    whenNew(SizeBasedRollingCondition.class).withAnyArguments().thenReturn(sizeBasedRollingConditionMock);
    when(avroFileNameMock.getFileName().toString()).thenReturn(AVRO_FILE_NAME);
    when(avroFileNameMock.getFileSystem().getSeparator()).thenReturn("/");
  }
  
  private void wireUpMocks() throws Exception {
    when(avroFileNameMock.getParent()).thenReturn(parentDirMock);
    when(Files.newDirectoryStream(parentDirMock)).thenReturn(dirContentsMock);
    when(dirContentsMock.iterator()).thenReturn(dirContentsIteratorMock);
  }

  @Test
  public void whenThePolicyIsInitialized_thenSizeBasedConditionIsBuiltWithTheConfiguredRollingSize() throws Exception {
    whenThePolicyIsInitialized();

    verifyNew(SizeBasedRollingCondition.class).withArguments(DEFAULT_ROLLING_POLICY_CONFIGURATION.getRollingSizeInMb());
  }

  @Test
  public void whenThePolicyIsInitialized_thenTheRollingIndexContinuesWhereWeLeftOff() {
    givenTheMaximumIndexInTheDirIs20();
    
    whenThePolicyIsInitialized();
    
    thenTheRollingIndexContinuesWhereWeLeftOff();
  }
  
  @Test
  public void shouldRolloverDecisionIsDelegatedToConditions() {
    givenThePolicyIsInitialized();
    
    timeAndSizeBasedRollingPolicyUnderTest.shouldRollover(null);
    
    thenDecisionIsDelegatedToConditions();
  }
  
  @Test
  public void whenTheNextRolledFileNameIsRetrieved_thenTheObtainedNameFollowsTheExpectedPattern() {
    givenThePolicyIsInitialized();

    final Path generatedName = timeAndSizeBasedRollingPolicyUnderTest.getNextRolledFileName();

    thenObtainedNameFollowsTheExpectedPattern(generatedName);
  }
  
  @Test
  public void whenTheNextRolledFileNameIsRetrieved_andTimeBasedRollIsDue_thenRolledFileNameDateIsYesterday() {
    givenThePolicyIsInitialized();
    givenTimeBasedRollIsDue();
    
    final DateTime date = getDateTimeFrom(timeAndSizeBasedRollingPolicyUnderTest.getNextRolledFileName());
    
    assertThat(date, is(equalTo(DateTime.now().minusDays(1).withTimeAtStartOfDay())));
  }
  
  @Test
  public void whenTheNextRolledFileNameIsRetrieved_andTimeBasedRollIsNotDue_thenRolledFileNameDateIsToday() {
    givenThePolicyIsInitialized();
    
    final DateTime date = getDateTimeFrom(timeAndSizeBasedRollingPolicyUnderTest.getNextRolledFileName());
    
    assertThat(date, is(equalTo(DateTime.now().withTimeAtStartOfDay())));
  }

  @Test
  public void whenTheNextRolledFileNameIsRetrieved_andTimeBasedRollIsDue_thenTheIndexOfTheRolledFileNameIsResetToZeroInTheNextCall() {
    givenThePolicyIsInitialized();
    givenTimeBasedRollIsDue();
    givenFiledIsRolled();
    givenTimeBasedRollIsNotDue();

    final int index = getIndexFrom(timeAndSizeBasedRollingPolicyUnderTest.getNextRolledFileName());

    assertThat(index, is(equalTo(0)));
  }
  
  @Test
  public void whenTheNextRolledFileNameIsRetrieved_andTimeBasedRollIsNotDue_thenTheIndexOfTheRolledFileNameIsIncreasedByOne() {
    givenThePolicyIsInitialized();
    givenTimeBasedRollIsNotDue();
    final int initialIndex = getIndexFrom(timeAndSizeBasedRollingPolicyUnderTest.getNextRolledFileName());

    final int finalIndex = getIndexFrom(timeAndSizeBasedRollingPolicyUnderTest.getNextRolledFileName());
    
    assertThat(finalIndex, is(equalTo(initialIndex + 1)));
  }

  @Test
  public void whenTheNextRolledFileNameIsRetrieved_thenInfoAboutARolloverEventIsPropagatedToConditions() {
    givenThePolicyIsInitialized();

    timeAndSizeBasedRollingPolicyUnderTest.getNextRolledFileName();

    thenInfoAboutTheEventIsPropagatedToConditions();
  }

  private void givenTheMaximumIndexInTheDirIs20() {
    when(dirContentsIteratorMock.hasNext()).thenReturn(true, true, true, false);
    when(dirContentsIteratorMock.next()).thenReturn(Paths.get("blah.blop"),
                                                    Paths.get(AVRO_FILE_NAME + "-34.20.log"),
                                                    Paths.get(AVRO_FILE_NAME + "12.16.log"));
  }
  
  private void givenThePolicyIsInitialized() {
    timeAndSizeBasedRollingPolicyUnderTest = new TimeAndSizeBasedRollingPolicy(DEFAULT_ROLLING_POLICY_CONFIGURATION);
    timeAndSizeBasedRollingPolicyUnderTest.registerAvroFileName(avroFileNameMock);
  }
  
  private void givenTimeBasedRollIsDue() {
    when(timeBasedRollingConditionMock.lastRolloverHappenedBeforeToday()).thenReturn(true);
  }
  
  private void givenFiledIsRolled() {
    timeAndSizeBasedRollingPolicyUnderTest.getNextRolledFileName();
  }

  private void givenTimeBasedRollIsNotDue() {
    when(timeBasedRollingConditionMock.lastRolloverHappenedBeforeToday()).thenReturn(false);
  }

  private void thenTheRollingIndexContinuesWhereWeLeftOff() {
    final int nextIndex = getIndexFrom(timeAndSizeBasedRollingPolicyUnderTest.getNextRolledFileName());
    assertThat(nextIndex, is(equalTo(20 + 1)));
  }

  private void whenThePolicyIsInitialized() {
    givenThePolicyIsInitialized();
  }

  private void thenDecisionIsDelegatedToConditions() {
    verify(timeBasedRollingConditionMock).lastRolloverHappenedBeforeToday();
    verify(sizeBasedRollingConditionMock).sizeThresholdHasBeenHit();
  }
  
  private void thenObtainedNameFollowsTheExpectedPattern(final Path generatedName) {
    assertThat(generatedName, matchesRegex("[a-zA-Z/]+-\\d{4}-\\d{2}-\\d{2}\\.\\d+\\.log"));
  }

  private void thenInfoAboutTheEventIsPropagatedToConditions() {
    verify(timeBasedRollingConditionMock).signalRollover();
    verify(sizeBasedRollingConditionMock).signalRollover();
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
  
  private DateTime getDateTimeFrom(final Path fileName) {
    final java.util.regex.Matcher dateTimeMatcher = Pattern.compile(avroFileNameMock.getFileName().toString()
                                                                    + ".+(\\d{4}-\\d{2}-\\d{2})\\.\\d+\\.log$")
                                                           .matcher(fileName.getFileName().toString());
    return dateTimeMatcher.find() ? DateTime.parse(dateTimeMatcher.group(1)) : new DateTime(0);
  }

  private int getIndexFrom(final Path fileName) {
    final java.util.regex.Matcher fileIndexMatcher = Pattern.compile(avroFileNameMock.getFileName().toString()
                                                                     + ".+\\d{2}\\.(\\d+)").matcher(fileName.getFileName().toString());
    return fileIndexMatcher.find() ? Integer.parseInt(fileIndexMatcher.group(1)) : Integer.MIN_VALUE;
  }
}
