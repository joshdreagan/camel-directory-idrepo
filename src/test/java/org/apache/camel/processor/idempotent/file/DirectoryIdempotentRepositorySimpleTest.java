package org.apache.camel.processor.idempotent.file;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectoryIdempotentRepositorySimpleTest {

  private static final Logger log = LoggerFactory.getLogger(DirectoryIdempotentRepositorySimpleTest.class);

  private static Path repoDirectory;
  private static String repoId;

  @BeforeClass
  public static void beforeClass() throws Exception {
    repoDirectory = Files.createTempDirectory(DirectoryIdempotentRepositorySimpleTest.class.getSimpleName());
    repoId = "repository";
  }

  @AfterClass
  public static void afterClass() throws Exception {
    Files.walkFileTree(repoDirectory, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.deleteIfExists(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.deleteIfExists(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  @Test
  public void testCleanupFileModifiedTime() throws Exception {
    int maxRepoSize = 10;
    long cleanupPeriod = 5000L;
    DirectoryIdempotentRepository boundedIdempotentRepository = new DirectoryIdempotentRepository(repoDirectory, repoId, maxRepoSize, cleanupPeriod, true);
    boundedIdempotentRepository.doStart();
    for (int i = 0; i < maxRepoSize * 2; ++i) {
      boundedIdempotentRepository.add(String.valueOf(i));
      Thread.sleep(1001L); // This is needed for filesystems that don't support millisecond mtime resolution.
    }

    // Give the cleanup thread some time to kick off and then clean up.
    final AtomicLong numFiles = new AtomicLong(Integer.MAX_VALUE);
    Awaitility.await().atMost(cleanupPeriod * 2, TimeUnit.MILLISECONDS).pollInterval(1000L, TimeUnit.MILLISECONDS).until(() -> {
      numFiles.set(Files.list(repoDirectory.resolve(repoId)).count());
      log.info(String.format("Found %s files on this check.", numFiles));
      return numFiles.longValue() <= maxRepoSize;
    });

    boundedIdempotentRepository.doStop();
    Assert.assertTrue(numFiles.longValue() <= maxRepoSize);
    Path oldest = Files.list(repoDirectory.resolve(repoId))
            .max((Path item1, Path item2) -> {
              try {
                return Files.getLastModifiedTime(item2).compareTo(Files.getLastModifiedTime(item1));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
            .get();
    log.info(String.format("Oldest repository item: [%s].", oldest));
    Assert.assertTrue(oldest.endsWith("10"));
  }

  @Test
  public void testCleanupFileContentTime() throws Exception {
    int maxRepoSize = 10;
    long cleanupPeriod = 5000L;
    DirectoryIdempotentRepository boundedIdempotentRepository = new DirectoryIdempotentRepository(repoDirectory, repoId, maxRepoSize, cleanupPeriod, false);
    boundedIdempotentRepository.doStart();
    for (int i = 0; i < maxRepoSize * 2; ++i) {
      boundedIdempotentRepository.add(String.valueOf(i));
      Thread.sleep(1L);
    }

    // Give the cleanup thread some time to kick off and then clean up.
    final AtomicLong numFiles = new AtomicLong(Integer.MAX_VALUE);
    Awaitility.await().atMost(cleanupPeriod * 2, TimeUnit.MILLISECONDS).pollInterval(1000L, TimeUnit.MILLISECONDS).until(() -> {
      numFiles.set(Files.list(repoDirectory.resolve(repoId)).count());
      log.info(String.format("Found %s files on this check.", numFiles));
      return numFiles.longValue() <= maxRepoSize;
    });

    boundedIdempotentRepository.doStop();
    Assert.assertTrue(numFiles.longValue() <= maxRepoSize);
    Path oldest = Files.list(repoDirectory.resolve(repoId))
            .max((Path item1, Path item2) -> {
              try {
                return Long.compare(Long.valueOf(new String(Files.readAllBytes(item2), DirectoryIdempotentRepository.DEFAULT_ENCODING)), Long.valueOf(new String(Files.readAllBytes(item1), DirectoryIdempotentRepository.DEFAULT_ENCODING)));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
            .get();
    log.info(String.format("Oldest repository item: [%s].", oldest));
    Assert.assertTrue(oldest.endsWith("10"));
  }
}
