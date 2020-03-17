package org.apache.camel.processor.idempotent.file;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.LoggingLevel;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectoryIdempotentRepositoryCamelTest extends CamelTestSupport {
  
  private static final Logger log = LoggerFactory.getLogger(DirectoryIdempotentRepositoryCamelTest.class);

  private static final String ID_HEADER = "UniqueID";
  private static final String FAILURE_HEADER = "ForceFailure";
  
  private Path repoDirectory;
  private String repoId;
  private DirectoryIdempotentRepository idempotentRepository;
  private DirectoryIdempotentRepository idempotentRepository2;
  
  @Override
  protected void doPreSetup() throws Exception {
    super.doPreSetup();
    
    repoDirectory = Files.createTempDirectory(this.getClass().getSimpleName());
    repoId = "repository";
    
    idempotentRepository = new DirectoryIdempotentRepository(repoDirectory, repoId);
    idempotentRepository2 = new DirectoryIdempotentRepository(repoDirectory, repoId);
  }

  @Override
  protected void doPostTearDown() throws Exception {
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
    
    super.doPostTearDown();
  }

  @Override
  protected RoutesBuilder createRouteBuilder() throws Exception {
    return new RouteBuilder() {
      @Override
      public void configure() throws Exception {
        errorHandler(deadLetterChannel("direct:printExceptionMessage"));
        
        from("direct:printExceptionMessage")
          .transform(simple("${exception.message}"))
          .log(LoggingLevel.ERROR, log, "${body}")
        ;
        
        from("direct:invoke")
          .idempotentConsumer(header(ID_HEADER), idempotentRepository)
            .log(LoggingLevel.INFO, log, String.format("Routing message: route=[%s], key=[${headers.%s}]", "direct:invoke", ID_HEADER))
            .filter(header(FAILURE_HEADER))
              .throwException(RuntimeCamelException.class, String.format("The [%s] header is set to [true].", FAILURE_HEADER))
            .end()
            .to("mock:accepted")
          .end()
        ;
        
        from("direct:invoke2")
          .idempotentConsumer(header(ID_HEADER), idempotentRepository2)
            .log(LoggingLevel.INFO, log, String.format("Routing message: route=[%s], key=[${headers.%s}]", "direct:invoke2", ID_HEADER))
            .filter(header(FAILURE_HEADER))
              .throwException(RuntimeCamelException.class, String.format("The [%s] header is set to [true].", FAILURE_HEADER))
            .end()
            .to("mock:accepted2")
          .end()
        ;
      }
    };
  }
  
  @Test
  public void testSameRoute() throws Exception {
    MockEndpoint mock = getMockEndpoint("mock:accepted");
    mock.expectedMessageCount(1);
    
    String message = "foo";
    String uniqueId = Thread.currentThread().getStackTrace()[1].getMethodName();
    Map<String, Object> headers = new HashMap<>();
    headers.put(ID_HEADER, uniqueId);
    template.sendBodyAndHeaders("direct:invoke", message, headers);
    template.sendBodyAndHeaders("direct:invoke", message, headers);
    
    MockEndpoint.assertIsSatisfied(mock);
  }
  
  @Test
  public void testDifferentRoute() throws Exception {
    MockEndpoint mock = getMockEndpoint("mock:accepted");
    mock.expectedMessageCount(1);

    MockEndpoint mock2 = getMockEndpoint("mock:accepted2");
    mock2.expectedMessageCount(0);
    
    String message = "foo";
    String uniqueId = Thread.currentThread().getStackTrace()[1].getMethodName();
    Map<String, Object> headers = new HashMap<>();
    headers.put(ID_HEADER, uniqueId);
    template.sendBodyAndHeaders("direct:invoke", message, headers);
    template.sendBodyAndHeaders("direct:invoke2", message, headers);
    
    MockEndpoint.assertIsSatisfied(mock, mock2);
  }
  
  @Test
  public void testFailureSameRoute() throws Exception {
    MockEndpoint mock = getMockEndpoint("mock:accepted");
    mock.expectedMessageCount(1);
    
    String message = "foo";
    String uniqueId = Thread.currentThread().getStackTrace()[1].getMethodName();
    Map<String, Object> headers = new HashMap<>();
    headers.put(ID_HEADER, uniqueId);
    headers.put(FAILURE_HEADER, true);
    try {
      template.sendBodyAndHeaders("direct:invoke", message, headers);
    } catch (CamelExecutionException e) {}
    headers.put(FAILURE_HEADER, false);
    template.sendBodyAndHeaders("direct:invoke", message, headers);
    
    MockEndpoint.assertIsSatisfied(mock);
  }
  
  @Test
  public void testFailureDifferentRoutes() throws Exception {
    MockEndpoint mock = getMockEndpoint("mock:accepted");
    mock.expectedMessageCount(0);

    MockEndpoint mock2 = getMockEndpoint("mock:accepted2");
    mock2.expectedMessageCount(1);
    
    String message = "foo";
    String uniqueId = Thread.currentThread().getStackTrace()[1].getMethodName();
    Map<String, Object> headers = new HashMap<>();
    headers.put(ID_HEADER, uniqueId);
    headers.put(FAILURE_HEADER, true);
    try {
      template.sendBodyAndHeaders("direct:invoke", message, headers);
    } catch (CamelExecutionException e) {}
    headers.put(FAILURE_HEADER, false);
    template.sendBodyAndHeaders("direct:invoke2", message, headers);
    
    MockEndpoint.assertIsSatisfied(mock, mock2);
  }
}
