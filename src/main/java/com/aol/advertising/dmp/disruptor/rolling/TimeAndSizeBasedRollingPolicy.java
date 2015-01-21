package com.aol.advertising.dmp.disruptor.rolling;

import static java.lang.Math.max;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.specific.SpecificRecord;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aol.advertising.dmp.disruptor.api.RollingPolicy;

/**
 * Rolled files names are indexed beginning at 0.
 * <p>
 * Two criteria for rolling:
 * <ul>
 * <li>Time-based: Destination file should be rolled every day at midnight. File index reset to 0</li>
 * <li>Size-based: Destination file should be rolled when size > {@code rolloverTriggeringSizeInMB}</li>
 * </ul>
 */
public class TimeAndSizeBasedRollingPolicy implements RollingPolicy {

  private static final Logger log = LoggerFactory.getLogger(TimeAndSizeBasedRollingPolicy.class);

  private static final String ROLLED_FILENAME_DATETIME_PATTERN = "yyyy-MM-dd";
  private static final DateTimeFormatter dateTimeFormatterForRolledFiles = DateTimeFormat.forPattern(ROLLED_FILENAME_DATETIME_PATTERN);
  // Don't substitute the pattern with a non-greedy matcher (i.e. "\\..+?$") It leads to some strange behavior
  private static final Pattern FILE_EXTENSION_PATTERN = Pattern.compile("\\.[^.]+$");

  private final TimeBasedRollingCondition timeBasedRollingCondition;
  private final SizeBasedRollingCondition sizeBasedRollingCondition;
  private final Path avroFileName;

  private int currentRollingIndex;

  public TimeAndSizeBasedRollingPolicy(int rolloverTriggeringSizeInMB, final Path avroFileName) {
    this.timeBasedRollingCondition = new TimeBasedRollingCondition();
    this.sizeBasedRollingCondition = new SizeBasedRollingCondition(avroFileName, rolloverTriggeringSizeInMB);
    this.avroFileName = avroFileName;

    init();
  }

  @Override
  public boolean shouldRollover(final Path _, final SpecificRecord avroRecord) {
    return timeBasedRollingCondition.rolloverShouldHappen() || sizeBasedRollingCondition.rolloverShouldHappen();
  }

  @Override
  // Format: <path_to_file><file_name_minus_extension>-yyyy-MM-dd.index.log
  public Path getNextRolledFileName(final Path _) {
    return Paths.get(avroFileName.getParent().toString(), avroFileName.getFileSystem().getSeparator()
                                                          + removeFileExtensionFrom(avroFileName)
                                                          + "-"
                                                          + dateTimeFormatterForRolledFiles.print(DateTime.now())
                                                          + "."
                                                          + currentRollingIndex
                                                          + ".log");
  }


  @Override
  public void signalRolloverOf(final Path _) {
    final Path rolledFileName = getNextRolledFileName(avroFileName);
    updateCurrentRollingIndex();
    timeBasedRollingCondition.signalRollover();
    sizeBasedRollingCondition.signalFiledRolledTo(rolledFileName);
  }

  private void updateCurrentRollingIndex() {
    if (timeBasedRollingCondition.rolloverShouldHappen()) {
      currentRollingIndex = 0;
    } else {
      currentRollingIndex++;
    }
  }

  private void init() {
    try {
      determineInitialRollingIndex();
    } catch (IOException ioe) {
      log.error("Could not initialize rolling policy", ioe);
    }
  }

  private void determineInitialRollingIndex() throws IOException {
    currentRollingIndex = getHighestIndexFromArchivedFilesInDir() + 1;
  }

  private int getHighestIndexFromArchivedFilesInDir() throws IOException {
    int highestIndex = Integer.MIN_VALUE;
    try (DirectoryStream<Path> dirContents = Files.newDirectoryStream(avroFileName.getParent())) {
      for (Path archivedFileName : dirContents) {
        highestIndex = max(highestIndex, getIndexFrom(archivedFileName.getFileName().toString()));
      }
    }
    return max(highestIndex, -1);
  }

  private int getIndexFrom(final String archivedFileName) {
    final Matcher fileIndexMatcher = Pattern.compile(removeFileExtensionFrom(avroFileName) + ".+\\d{2}\\.(\\d+)").matcher(archivedFileName);
    return fileIndexMatcher.find() ? Integer.parseInt(fileIndexMatcher.group(1)) : Integer.MIN_VALUE;
  }

  private String removeFileExtensionFrom(final Path avroFileName) {
    return FILE_EXTENSION_PATTERN.matcher(avroFileName.getFileName().toString()).replaceFirst("");
  }
}
