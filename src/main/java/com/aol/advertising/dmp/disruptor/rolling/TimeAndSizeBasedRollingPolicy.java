package com.aol.advertising.dmp.disruptor.rolling;

import static java.lang.Math.max;
import static java.lang.Math.min;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.specific.SpecificRecord;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aol.advertising.dmp.disruptor.api.RollingPolicy;

/**
 * Two criteria for rolling:
 * <ul>
 * <li>Time-based: Destination file should be rolled every day at midnight</li>
 * <li>Size-based: Destination file should be rolled when size > {@code rolloverTriggeringSizeInMB}.
 * Names of archived files will be indexed in the range [0, {@code rollingIndexRange}] and wrap when index
 * > {@code indexPeriod}</li>
 * </ul>
 */
public class TimeAndSizeBasedRollingPolicy implements RollingPolicy {

  private static final Logger log = LoggerFactory.getLogger(TimeAndSizeBasedRollingPolicy.class);

  private static final String ROLLED_FILENAME_DATETIME_PATTERN = "yyyy-MM-dd";
  private static final DateTimeFormatter dateTimeFormatterForRolledFiles = DateTimeFormat.forPattern(ROLLED_FILENAME_DATETIME_PATTERN);
  private static final Pattern FILE_EXTENSION_PATTERN = Pattern.compile("\\..+?$");

  private final TimeBasedRollingCondition timeBasedRollingCondition;
  private final SizeBasedRollingCondition sizeBasedRollingCondition;
  private final int rollingIndexRange;
  private final File avroFileName;

  private int currentRollingIndex;

  public TimeAndSizeBasedRollingPolicy(int rolloverTriggeringSizeInMB, int rollingIndexRange, final File avroFileName) {
    this.timeBasedRollingCondition = new TimeBasedRollingCondition();
    this.sizeBasedRollingCondition = new SizeBasedRollingCondition(avroFileName.length(), rolloverTriggeringSizeInMB);
    this.rollingIndexRange = rollingIndexRange;
    this.avroFileName = avroFileName;

    init();
  }

  @Override
  public boolean shouldRollover(final File _, final SpecificRecord avroRecord) {
    return timeBasedRollingCondition.rolloverShouldHappen() || sizeBasedRollingCondition.rolloverShouldHappen(avroFileName);
  }

  @Override
  // Format: <path_to_file><file_name_minus_extension>-yyyy-MM-dd.index.log
  public String getNextRolledFileName(final File _) {
    final String nextRolledFileName = avroFileName.getParent()
                                      + File.separatorChar
                                      + getFileNameWithoutExtension(avroFileName.getName())
                                      + "-"
                                      + dateTimeFormatterForRolledFiles.print(UTCDateTime.now())
                                      + "."
                                      + currentRollingIndex
                                      + ".log";
    getNextIndexInRange();
    return nextRolledFileName;
  }


  @Override
  public void signalRolloverOf(final File avroFileName) {
    timeBasedRollingCondition.signalRollover();
    sizeBasedRollingCondition.signalRolloverOf(avroFileName);
  }

  private void init() {
    try {
      determineInitialRollingIndex();
    } catch (SecurityException se) {
      log.error("Could not initialize rolling policy", se);
    }
  }

  private void determineInitialRollingIndex() throws SecurityException {
    getHighestIndexInRangeFromDir();
    getNextIndexInRange();
  }

  private void getHighestIndexInRangeFromDir() {
    int highestIndex = Integer.MIN_VALUE;
    for (File archivedFileName : avroFileName.getParentFile().listFiles()) {
      highestIndex = max(highestIndex, getIndexFrom(archivedFileName.getName()));
    }
    currentRollingIndex = min(max(highestIndex, -1), rollingIndexRange);
  }

  private int getIndexFrom(final String archivedFileName) {
    final String avroFileNameWithoutExtension = getFileNameWithoutExtension(avroFileName.getName());
    final Matcher fileNameMatcher = Pattern.compile(avroFileNameWithoutExtension + ".+\\d{2}\\.(\\d+)").matcher(archivedFileName);
    return fileNameMatcher.find() ? Integer.parseInt(fileNameMatcher.group(1)) : Integer.MIN_VALUE;
  }

  private String getFileNameWithoutExtension(final String fileName) {
    final Matcher fileNameMatcher = FILE_EXTENSION_PATTERN.matcher(fileName);
    return fileNameMatcher.replaceAll("");
  }

  private void getNextIndexInRange() {
    currentRollingIndex = ++currentRollingIndex % (rollingIndexRange + 1);
  }
}
