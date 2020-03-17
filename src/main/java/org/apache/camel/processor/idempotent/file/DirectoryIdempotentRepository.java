package org.apache.camel.processor.idempotent.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.camel.spi.IdempotentRepository;
import org.apache.camel.support.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectoryIdempotentRepository extends ServiceSupport implements IdempotentRepository<String> {

  private static final Logger log = LoggerFactory.getLogger(DirectoryIdempotentRepository.class);

  public static final String DEFAULT_ENCODING = "UTF-8";
  public static final Path DEFAULT_REPO_DIRECTORY = Paths.get(System.getProperty("java.io.tmpdir"));
  public static final int DEFAULT_MAX_REPO_SIZE = 1000;
  public static final long DEFAULT_CLEANUP_PERIOD = 30000L;
  public static final boolean DEFAULT_USE_FILE_MODIFIED_TIME = true;

  private final Path repoDirectory;
  private final String repoId;
  private final int maxRepoSize;
  private final long cleanupPeriod;
  private final boolean useFileModifiedTime;

  private Path _repoPath;
  private ScheduledExecutorService _cleanupService;

  public DirectoryIdempotentRepository(String repoId) {
    this(DEFAULT_REPO_DIRECTORY, repoId);
  }

  public DirectoryIdempotentRepository(Path repoDirectory, String repoId) {
    this(repoDirectory, repoId, DEFAULT_MAX_REPO_SIZE, DEFAULT_CLEANUP_PERIOD, DEFAULT_USE_FILE_MODIFIED_TIME);
  }

  public DirectoryIdempotentRepository(Path repoDirectory, String repoId, int maxRepoSize, long cleanupPeriod, boolean useFileModifiedTime) {
    Objects.requireNonNull(repoDirectory, "The 'repoDirectory' parameter must not be null.");
    if (Objects.requireNonNull(repoId, "The 'repoId' parameter must not be null.").trim().isEmpty()) {
      throw new IllegalArgumentException("The 'repoId' parameter must not be empty.");
    }
    if (maxRepoSize < 0) {
      throw new IllegalArgumentException("The 'maxRepoSize' parameter must be greater than 0.");
    }
    if (cleanupPeriod < 1000) {
      throw new IllegalArgumentException("The 'cleanupPeriod' parameter must be greater than 999.");
    }

    this.repoDirectory = repoDirectory;
    this.repoId = repoId;
    this.maxRepoSize = maxRepoSize;
    this.cleanupPeriod = cleanupPeriod;
    this.useFileModifiedTime = useFileModifiedTime;
  }

  public Path getRepoDirectory() {
    return repoDirectory;
  }

  public String getRepoId() {
    return repoId;
  }

  public int getMaxRepoSize() {
    return maxRepoSize;
  }

  public long getCleanupPeriod() {
    return cleanupPeriod;
  }

  public boolean isUseFileModifiedTime() {
    return useFileModifiedTime;
  }

  @Override
  protected void doStop() throws Exception {
    if (_cleanupService != null) {
      log.debug(String.format("Stopping cleanup service for repository [%s].", _repoPath));
      _cleanupService.shutdown();
      _cleanupService = null;
    }
  }

  @Override
  protected void doStart() throws Exception {
    if (useFileModifiedTime) {
      Path precisionTestOne = Files.createTempFile(repoDirectory, "precisionTest", "");
      Thread.sleep(1L);
      Path precisionTestTwo = Files.createTempFile(repoDirectory, "precisionTest", "");
      long delta = Files.getLastModifiedTime(precisionTestTwo).toMillis() - Files.getLastModifiedTime(precisionTestOne).toMillis();
      if (delta == 0) {
        log.warn(String.format("Filesystem [%s] does not support millisecond precision. This could impact repository cleanup operations.", repoDirectory.getRoot().toAbsolutePath().toString()));
      }
      Files.deleteIfExists(precisionTestOne);
      Files.deleteIfExists(precisionTestTwo);
    }

    _repoPath = repoDirectory.resolve(repoId);
    try {
      Files.createDirectories(repoDirectory);
      Files.createDirectory(_repoPath);
      log.debug(String.format("Created repository: [%s].", _repoPath));
    } catch (FileAlreadyExistsException e) {
      log.debug(String.format("Repository already exists: [%s].", _repoPath));
    }

    _cleanupService = Executors.newSingleThreadScheduledExecutor();
    log.debug(String.format("Starting cleanup service for repository [%s].", _repoPath));
    _cleanupService.scheduleWithFixedDelay(() -> {
      try {
        log.debug(String.format("Cleaning repository [%s].", _repoPath));
        Files.list(_repoPath)
                .sorted((Path item1, Path item2) -> {
                  try {
                    if (useFileModifiedTime) {
                      return Files.getLastModifiedTime(item2).compareTo(Files.getLastModifiedTime(item1));
                    } else {
                      return Long.compare(Long.valueOf(new String(Files.readAllBytes(item2), DEFAULT_ENCODING)), Long.valueOf(new String(Files.readAllBytes(item1), DEFAULT_ENCODING)));
                    }
                  } catch (IOException e) {
                    log.warn(String.format("Unable to stat files [%s, %s]. Skipping.", item1, item2), e);
                    return Integer.MAX_VALUE;
                  }
                })
                .skip(maxRepoSize)
                .forEachOrdered((Path item) -> {
                  try {
                    log.debug(String.format("Deleting file [%s].", item));
                    Files.deleteIfExists(item);
                  } catch (IOException e) {
                    log.warn(String.format("Unable to delete file [%s]. Skipping.", item), e);
                  }
                });
      } catch (Throwable t) {
        log.warn(String.format("Error cleaning repository [%s].", _repoPath), t);
      }
    }, cleanupPeriod, cleanupPeriod, TimeUnit.MILLISECONDS);
  }

  @Override
  public boolean add(String key) {
    try {
      if (useFileModifiedTime) {
        Files.createFile(_repoPath.resolve(key));
      } else {
        Files.write(_repoPath.resolve(key), Long.toString(System.currentTimeMillis()).getBytes(DEFAULT_ENCODING), StandardOpenOption.CREATE_NEW);
      }
      log.debug(String.format("Added key to repository: [%s].", key));
      return true;
    } catch (FileAlreadyExistsException e) {
      log.debug(String.format("Repository already contains key: [%s]. Skipping.", key));
      return false;
    } catch (Exception e) {
      log.debug(String.format("Error adding key to repository: [%s].", key), e);
      return false;
    }
  }

  @Override
  public void clear() {
    try {
      log.debug(String.format("Clearing repository [%s].", _repoPath));
      long now = System.currentTimeMillis();
      try (Stream<Path> s = Files.list(_repoPath)) {
        s.filter((Path item) -> {
          try {
            if (useFileModifiedTime) {
              return Files.isRegularFile(item) && (Files.getLastModifiedTime(item).toMillis() <= now);
            } else {
              return Files.isRegularFile(item) && (Long.valueOf(new String(Files.readAllBytes(item), DEFAULT_ENCODING)) <= now);
            }
          } catch (IOException e) {
            log.warn(String.format("Unable to stat file [%s]. Skipping.", item), e);
            return false;
          }
        }).forEach((Path item) -> {
          try {
            Files.deleteIfExists(item);
          } catch (IOException e) {
            log.warn(String.format("Unable to delete file [%s]. Skipping.", item), e);
          }
        });
      }
    } catch (Exception e) {
      log.debug(String.format("Error clearing repository [%s].", _repoPath), e);
    }
  }

  @Override
  public boolean contains(String key) {
    boolean contains = Files.exists(_repoPath.resolve(key));
    if (contains) {
      log.debug(String.format("Repository contains key: [%s].", key));
    } else {
      log.debug(String.format("Repository does not contain key: [%s].", key));
    }
    return contains;
  }

  @Override
  public boolean confirm(String key) {
    return true;
  }

  @Override
  public boolean remove(String key) {
    try {
      boolean removed = Files.deleteIfExists(_repoPath.resolve(key));
      if (removed) {
        log.debug(String.format("Removed key from repository: [%s].", key));
      } else {
        log.debug(String.format("Repository does not contain key: [%s]. Skipping", key));
      }
      return removed;
    } catch (Exception e) {
      log.debug(String.format("Error removing key from repository: [%s].", key), e);
      return false;
    }
  }
}
