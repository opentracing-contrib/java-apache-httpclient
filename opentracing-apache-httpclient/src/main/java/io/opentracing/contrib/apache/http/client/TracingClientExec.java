package io.opentracing.contrib.apache.http.client;

import static io.opentracing.contrib.apache.http.client.Constants.PARENT_CONTEXT;

import io.opentracing.Scope;
import io.opentracing.Span;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpExecutionAware;
import org.apache.http.client.methods.HttpRequestWrapper;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.execchain.ClientExecChain;

import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;

/**
 * Tracing is added before {@link org.apache.http.impl.execchain.ProtocolExec} which is invoked as
 * the next to last. Note that {@link org.apache.http.impl.execchain.RedirectExec} is invoked before
 * so this exec has to deal with redirects.
 *
 * @author Pavol Loffay
 */
public class TracingClientExec implements ClientExecChain {
  static final String COMPONENT_NAME = "apache-httpclient";

  /**
   * Id of {@link HttpClientContext#setAttribute(String, Object)} representing span associated with
   * the current client processing. Referenced span is local span not a span representing HTTP communication.
   */
  protected static final String SPAN_PROP = TracingHttpClientBuilder.class.getName() + ".activeSpan";
  /**
   * Tracing {@link ClientExecChain} is executed after redirect exec, so on redirects it is called
   * multiple times. This is used as an id for {@link HttpClientContext#setAttribute(String, Object)}
   * to store number of redirects.
   */
  protected static final String REDIRECT_COUNT = TracingHttpClientBuilder.class.getName() + ".redirectCount";

  private final RedirectStrategy redirectStrategy;
  private final ClientExecChain requestExecutor;
  private final boolean redirectHandlingDisabled;

  private final Tracer tracer;
  private final List<ApacheClientSpanDecorator> spanDecorators;

  public TracingClientExec(
      ClientExecChain clientExecChain,
      RedirectStrategy redirectStrategy,
      boolean redirectHandlingDisabled,
      Tracer tracer,
      List<ApacheClientSpanDecorator> spanDecorators) {
    this.requestExecutor = clientExecChain;
    this.redirectStrategy = redirectStrategy;
    this.redirectHandlingDisabled = redirectHandlingDisabled;
    this.tracer = tracer;
    this.spanDecorators = new ArrayList<>(spanDecorators);
  }

  @Override
  public CloseableHttpResponse execute(
      HttpRoute route,
      HttpRequestWrapper request,
      HttpClientContext clientContext,
      HttpExecutionAware execAware) throws IOException, HttpException {

    Span localSpan = clientContext.getAttribute(SPAN_PROP, Span.class);
    CloseableHttpResponse response = null;
    try {
      if (localSpan == null) {
        localSpan = handleLocalSpan(request, clientContext);
      }

      return (response = handleNetworkProcessing(localSpan, route, request, clientContext, execAware));
    } catch (Exception e) {
      localSpan.finish();
      throw e;
    } finally {
      if (response != null) {
        /**
         * This exec runs after {@link org.apache.http.impl.execchain.RedirectExec} which loops
         * until there is no redirect or reaches max redirect count.
         * {@link RedirectStrategy} is used to decide whether localSpan should be finished or not.
         * If there is a redirect localSpan is not finished and redirect is logged.
         */
        Integer redirectCount = clientContext.getAttribute(REDIRECT_COUNT, Integer.class);
        if (!redirectHandlingDisabled &&
            clientContext.getRequestConfig().isRedirectsEnabled() &&
            redirectStrategy.isRedirected(request, response, clientContext) &&
            ++redirectCount < clientContext.getRequestConfig().getMaxRedirects()) {

          clientContext.setAttribute(REDIRECT_COUNT, redirectCount);
        } else {
          localSpan.finish();
        }
      }
    }
  }

  protected Span handleLocalSpan(HttpRequest httpRequest, HttpClientContext clientContext) {
    Tracer.SpanBuilder spanBuilder = tracer.buildSpan(httpRequest.getRequestLine().getMethod())
        .withTag(Tags.COMPONENT.getKey(), COMPONENT_NAME);

    if (clientContext.getAttribute(PARENT_CONTEXT, SpanContext.class) != null) {
      spanBuilder.ignoreActiveSpan()
              .asChildOf(clientContext.getAttribute(PARENT_CONTEXT, SpanContext.class));
    }

    Span localSpan = spanBuilder.startManual();
    clientContext.setAttribute(SPAN_PROP, localSpan);
    clientContext.setAttribute(REDIRECT_COUNT, 0);
    return localSpan;
  }

  protected CloseableHttpResponse handleNetworkProcessing(
      Span parentScope,
      HttpRoute route,
      HttpRequestWrapper request,
      HttpClientContext clientContext,
      HttpExecutionAware execAware) throws IOException, HttpException {

    Scope redirectScope = tracer.buildSpan(request.getMethod())
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT)
        .asChildOf(parentScope)
        .startActive(true);
    tracer.inject(redirectScope.span().context(), Format.Builtin.HTTP_HEADERS, new HttpHeadersInjectAdapter(request));

    try {
      for (ApacheClientSpanDecorator decorator : spanDecorators) {
        decorator.onRequest(request, clientContext, redirectScope.span());
      }
      CloseableHttpResponse response = requestExecutor.execute(route, request, clientContext, execAware);
      for (ApacheClientSpanDecorator decorator : spanDecorators) {
        decorator.onResponse(response, clientContext, redirectScope.span());
      }
      return response;
    } catch (IOException | HttpException | RuntimeException e) {
      for (ApacheClientSpanDecorator decorator: spanDecorators) {
        decorator.onError(request, clientContext, e, redirectScope.span());
      }
      throw e;
    } finally {
      redirectScope.close();
    }
  }

}
