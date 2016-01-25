package com.aol.advertising.vulcan.rolling;

import static java.lang.Math.max;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

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

import com.aol.advertising.vulcan.api.rolling.RollingPolicy;
import com.aol.advertising.vulcan.exception.FileRollingException;
import com.aol.advertising.vulcan.rolling.RollingCondition.FileAwareRollingCondition;

/**
 * Rolled files names are indexed beginning at 0. File format:
 * &lt;filename_minus_extension&gt;-yyyy-MM-dd.&lt;index&gt;.log
 * <p>
 * Two criteria for rolling:
 * <ul>
 * <li>Time-based: Destination file should be rolled every day at midnight. File index reset to 0
 * </li>
 * <li>Size-based: Destination file should be rolled when size > {@code rolloverTriggeringSizeInMB}
 * </li>
 * </ul>
 * 
 * @author Jaime Nuche
 *
 */
public class TimeAndSizeBasedRollingPolicy implements RollingPolicy {

  private static final Logger log = LoggerFactory.getLogger(TimeAndSizeBasedRollingPolicy.class);

  private static final String ROLLED_FILENAME_DATETIME_PATTERN = "yyyy-MM-dd";
  private static final DateTimeFormatter dateTimeFormatterForRolledFiles =
      DateTimeFormat.forPattern(ROLLED_FILENAME_DATETIME_PATTERN);
  private static final Pattern FILE_EXTENSION_PATTERN = Pattern.compile("\\.[^.]+$");

  private final RollingCondition lastRolloverHappenedBeforeToday;
  private final FileAwareRollingCondition sizeThresholdHasBeenHit;

  private Path avroFilename;
  private int rollingIndex;

  public TimeAndSizeBasedRollingPolicy(TimeAndSizeBasedRollingPolicyConfig configuration) {
    this.lastRolloverHappenedBeforeToday = new TimeBasedRollingCondition();
    this.sizeThresholdHasBeenHit = new SizeBasedRollingCondition(configuration.getRollingSizeInMb());

    this.rollingIndex = 0;
  }

  @Override
  public void registerAvroFilename(Path avroFilename) {
    this.avroFilename = avroFilename;
    sizeThresholdHasBeenHit.registerAvroFileName(avroFilename);

    init();
  }

  @Override
  public boolean shouldRollover(SpecificRecord avroRecord) {
    return sizeThresholdHasBeenHit.shouldRollover() || lastRolloverHappenedBeforeToday.shouldRollover();
  }

  @Override
  public void rolloverAvroFile() throws FileRollingException {
    signalRolloverToConditions();
    renameAvroFile();
    updateRollingIndex();
  }

  private void renameAvroFile() throws FileRollingException {
    try {
      Files.move(avroFilename, getNextRolledFilename(), REPLACE_EXISTING);
    } catch (IOException e) {
      throw new FileRollingException("File \"" + getNextRolledFilename()
          + "\" could not be used to roll the current output Avro file");
    }
  }

  private Path getNextRolledFilename() {
    DateTime nextDateTimeToUse = selectNextDateTimeToUse();
    // @formatter:off
    Path nextRolledFilename = Paths.get(avroFilename.getParent().toString(),
                                        avroFilename.getFileSystem().getSeparator()
                                        + removeFileExtensionFrom(avroFilename)
                                        + "-"
                                        + dateTimeFormatterForRolledFiles.print(nextDateTimeToUse)
                                        + "."
                                        + rollingIndex
                                        + ".log");
    // @formatter:on
    return nextRolledFilename;
  }

  private DateTime selectNextDateTimeToUse() {
    if (lastRolloverHappenedBeforeToday.shouldRollover()) {
      return yesterday();
    } else {
      return DateTime.now();
    }
  }

  private DateTime yesterday() {
    return DateTime.now().minusDays(1);
  }

  private void updateRollingIndex() {
    if (lastRolloverHappenedBeforeToday.shouldRollover()) {
      rollingIndex = 0;
    } else {
      rollingIndex++;
    }
  }

  private void signalRolloverToConditions() {
    lastRolloverHappenedBeforeToday.signalRollover();
    sizeThresholdHasBeenHit.signalRollover();
  }

  private void init() {
    try {
      determineInitialRollingIndex();
    } catch (IOException ioe) {
      log.error("Could not initialize rolling policy. Resorting to defaults", ioe);
      rollingIndex = 0;
    }
  }

  private void determineInitialRollingIndex() throws IOException {
    rollingIndex = getHighestIndexFromArchivedFilesInDir() + 1;
  }

  private int getHighestIndexFromArchivedFilesInDir() throws IOException {
    int highestIndex = -1;
    try (DirectoryStream<Path> dirContents = Files.newDirectoryStream(avroFilename.getParent())) {
      for (Path archivedFileName : dirContents) {
        highestIndex = max(highestIndex, getIndexFrom(archivedFileName.getFileName().toString()));
      }
    }
    return highestIndex;
  }

  private int getIndexFrom(String archivedFileName) {
    Matcher fileIndexMatcher =
        Pattern.compile(removeFileExtensionFrom(avroFilename) + ".+\\d{2}\\.(\\d+)").matcher(archivedFileName);
    return fileIndexMatcher.find() ? Integer.parseInt(fileIndexMatcher.group(1)) : Integer.MIN_VALUE;
  }

  private String removeFileExtensionFrom(Path avroFileName) {
    return FILE_EXTENSION_PATTERN.matcher(avroFileName.getFileName().toString()).replaceFirst("");
  }
}
