package io.opentracing.contrib.apache;

import static io.opentracing.contrib.apache.TracingHttpClientBuilder.PARENT_CONTEXT;

import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.contrib.spanmanager.DefaultSpanManager;
import io.opentracing.contrib.spanmanager.SpanManager;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpException;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpExecutionAware;
import org.apache.http.client.methods.HttpRequestWrapper;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.execchain.ClientExecChain;

/**
 * Tracing is added before {@link org.apache.http.impl.execchain.ProtocolExec} which is invoked as
 * the next to last. Note that {@link org.apache.http.impl.execchain.RedirectExec} is invoked before
 * so this exec has to deal with redirects.
 *
 * @author Pavol Loffay
 */
public class TracingClientExec implements ClientExecChain {
  /**
   * Id of {@link HttpClientContext#setAttribute(String, Object)} representing span associated with
   * the current request.
   */
  protected static final String ACTIVE_SPAN = TracingHttpClientBuilder.class.getName() + ".activeSpan";
  /**
   * Tracing {@link ClientExecChain} is executed after redirect exec, so on redirects it is called
   * multiple times. This is used as an id for {@link HttpClientContext#setAttribute(String, Object)}
   * to store number of redirects.
   */
  protected static final String REDIRECT_COUNT = TracingHttpClientBuilder.class.getName() + ".redirectCount";

  private final RedirectStrategy redirectStrategy;
  private final ClientExecChain requestExecutor;
  private final boolean redirectHandlingDisabled;

  private final Tracer tracer = GlobalTracer.get();
  private final SpanManager spanManager = DefaultSpanManager.getInstance();
  private final List<ApacheClientSpanDecorator> spanDecorators;

  public TracingClientExec(ClientExecChain clientExecChain, RedirectStrategy redirectStrategy,
      boolean redirectHandlingDisabled, List<ApacheClientSpanDecorator> spanDecorators) {
    this.requestExecutor = clientExecChain;
    this.redirectStrategy = redirectStrategy;
    this.redirectHandlingDisabled = redirectHandlingDisabled;
    this.spanDecorators = new ArrayList<>(spanDecorators);
  }

  @Override
  public CloseableHttpResponse execute(HttpRoute route,
                                       HttpRequestWrapper request,
                                       HttpClientContext clientContext,
                                       HttpExecutionAware execAware)
      throws IOException, HttpException {

    Span span = clientContext.getAttribute(ACTIVE_SPAN, Span.class);
    CloseableHttpResponse response = null;
    try {
      // do not create spans for redirect
      if (span != null) {
        // original headers are not passed to redirect
        tracer.inject(span.context(), Format.Builtin.HTTP_HEADERS, new HttpHeadersInjectAdapter(request));
        return (response = requestExecutor.execute(route, request, clientContext, execAware));
      }

      Tracer.SpanBuilder spanBuilder = tracer.buildSpan(request.getRequestLine().getMethod())
          .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT);

      if (clientContext.getAttribute(PARENT_CONTEXT, SpanContext.class) != null) {
        spanBuilder.asChildOf(clientContext.getAttribute(PARENT_CONTEXT, SpanContext.class));
      } else if (spanManager.current().getSpan() != null) {
        spanBuilder.asChildOf(spanManager.current().getSpan());
      }

      span = spanBuilder.start();
      tracer.inject(span.context(), Format.Builtin.HTTP_HEADERS, new HttpHeadersInjectAdapter(request));
      clientContext.setAttribute(ACTIVE_SPAN, span);
      clientContext.setAttribute(REDIRECT_COUNT, 0);

      for (ApacheClientSpanDecorator decorator: spanDecorators) {
        decorator.onRequest(request, clientContext, span);
      }
      return (response = requestExecutor.execute(route, request, clientContext, execAware));
    } catch (IOException | HttpException | RuntimeException e) {
      for (ApacheClientSpanDecorator decorator: spanDecorators) {
        decorator.onError(request, clientContext, e, span);
      }
      throw e;
    } finally {
      if (response != null) {
        Integer redirectCount = clientContext.getAttribute(REDIRECT_COUNT, Integer.class);
        /**
         * This exec runs after {@link org.apache.http.impl.execchain.RedirectExec} which loops
         * until there is no redirect or reaches max redirect count.
         * {@link RedirectStrategy} is used to decide whether span should be finished or not.
         * If there is a redirect span is not finished and redirect is logged.
         */
        if (!redirectHandlingDisabled &&
            clientContext.getRequestConfig().isRedirectsEnabled() &&
            redirectStrategy.isRedirected(request, response, clientContext) &&
            ++redirectCount < clientContext.getRequestConfig().getMaxRedirects()) {

          clientContext.setAttribute(REDIRECT_COUNT, redirectCount);
          for (ApacheClientSpanDecorator decorator : spanDecorators) {
            decorator.onRedirect(response, clientContext, span);
          }
        } else {
          for (ApacheClientSpanDecorator decorator : spanDecorators) {
            decorator.onResponse(response, clientContext, span);
          }
          span.finish();
        }
      } else {
        span.finish();
      }
    }
  }
}
