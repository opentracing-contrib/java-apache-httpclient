package io.opentracing.contrib.apache.http.client;

/**
 * @author Pavol Loffay
 */
public final class Constants {

  private Constants() {}

  /**
   * SpanContext which will be used as a parent for created client span.
   */
  public static final String PARENT_CONTEXT = TracingHttpClientBuilder.class.getName() + ".parentSpanContext";
}
