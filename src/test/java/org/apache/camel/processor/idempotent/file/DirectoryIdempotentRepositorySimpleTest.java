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

  private static Path tempDir;

  @BeforeClass
  public static void beforeClass() throws Exception {
    tempDir = Files.createTempDirectory(DirectoryIdempotentRepositorySimpleTest.class.getSimpleName());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    Files.walkFileTree(tempDir, new SimpleFileVisitor<Path>() {
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
  public void testCleanup() throws Exception {
    Path repoDirectory = tempDir;
    String repoId = Thread.currentThread().getStackTrace()[1].getMethodName();
    int maxRepoSize = 10;
    long cleanupPeriod = 3000L;
    DirectoryIdempotentRepository repo = new DirectoryIdempotentRepository(repoDirectory, repoId, maxRepoSize, cleanupPeriod);

    repo.start();
    repo.add("key_-1");
    Thread.sleep(1001L); // This is needed for filesystems that don't support millisecond mtime resolution.
    for (int i = 0; i < repo.getMaxRepoSize(); ++i) {
      repo.add("key_" + i);
    }

    // Give the cleanup thread some time to kick off and then clean up.
    final AtomicLong numFiles = new AtomicLong(Integer.MAX_VALUE);
    Awaitility.await().atMost(repo.getCleanupPeriod() * 2, TimeUnit.MILLISECONDS).pollInterval(1L, TimeUnit.SECONDS).until(() -> {
      numFiles.set(Files.list(repo.getRepoDirectory().resolve(repo.getRepoId())).count());
      log.info(String.format("Found %s files on this check.", numFiles));
      return numFiles.longValue() <= repo.getMaxRepoSize();
    });
    repo.stop();

    Assert.assertTrue(numFiles.longValue() <= repo.getMaxRepoSize());
    Assert.assertFalse(repo.contains("key_-1"));
  }
}
