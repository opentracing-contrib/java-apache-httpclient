package io.opentracing.contrib.apache;

import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.contrib.apache.ApacheClientSpanDecorator.StandardTags;
import io.opentracing.contrib.spanmanager.DefaultSpanManager;
import io.opentracing.contrib.spanmanager.SpanManager;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.http.HttpException;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpExecutionAware;
import org.apache.http.client.methods.HttpRequestWrapper;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.execchain.ClientExecChain;

/**
 * @author Pavol Loffay
 */
public class TracingHttpClientBuilder extends org.apache.http.impl.client.HttpClientBuilder {
    /**
     * SpanContext which will be used as a parent for created client span.
     */
    public static final String PARENT_CONTEXT = TracingHttpClientBuilder.class.getName() + ".parentSpanContext";

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

    private RedirectStrategy redirectStrategy;
    private boolean redirectHandlingDisabled;

    private Tracer tracer;
    private SpanManager spanManager = DefaultSpanManager.getInstance();
    private List<ApacheClientSpanDecorator> spanDecorators;

    /**
     * When using this constructor tracer should be registered via
     * {@link GlobalTracer#register(Tracer)}.
     */
    public TracingHttpClientBuilder() {
        this(DefaultRedirectStrategy.INSTANCE,
                false,
                GlobalTracer.get(),
                Collections.<ApacheClientSpanDecorator>singletonList(
                    new ApacheClientSpanDecorator.StandardTags()));
    }

    /**
     * @param redirectStrategy redirect strategy, do not call
     * {@link HttpClientBuilder#disableRedirectHandling()}
     * @param redirectHandlingDisabled disable redirect strategy, do not call
     * {@link org.apache.http.impl.client.HttpClientBuilder#setRedirectStrategy(RedirectStrategy)}
     */
    public TracingHttpClientBuilder(RedirectStrategy redirectStrategy,
        boolean redirectHandlingDisabled) {
        this(redirectStrategy,
            redirectHandlingDisabled,
            GlobalTracer.get(),
            Collections.<ApacheClientSpanDecorator>singletonList(new StandardTags()));
    }

        /**
         * @param redirectStrategy redirect strategy, do not call
         * {@link HttpClientBuilder#disableRedirectHandling()}
         * @param redirectHandlingDisabled disable redirect strategy, do not call
         * {@link org.apache.http.impl.client.HttpClientBuilder#setRedirectStrategy(RedirectStrategy)}
         * @param tracer tracer instance
         * @param spanDecorators decorators
         */
    public TracingHttpClientBuilder(RedirectStrategy redirectStrategy,
                                    boolean redirectHandlingDisabled,
                                    Tracer tracer,
                                    List<ApacheClientSpanDecorator> spanDecorators) {
        this.redirectStrategy = redirectStrategy;
        this.redirectHandlingDisabled = redirectHandlingDisabled;
        this.tracer = tracer;
        this.spanDecorators = new ArrayList<>(spanDecorators);

        super.setRedirectStrategy(redirectStrategy);
        if (redirectHandlingDisabled) {
            super.disableRedirectHandling();
        }
    }

    public static TracingHttpClientBuilder create() {
        return new TracingHttpClientBuilder();
    }

    @Override
    protected ClientExecChain decorateProtocolExec(final ClientExecChain requestExecutor) {
        return new ClientExecChain() {
            @Override
            public CloseableHttpResponse execute(HttpRoute route, HttpRequestWrapper request,
                                                 HttpClientContext clientContext,
                                                 HttpExecutionAware execAware) throws IOException, HttpException {

                Span span = clientContext.getAttribute(ACTIVE_SPAN, Span.class);
                CloseableHttpResponse response = null;
                try {
                    // do not create spans for redirect
                    if (span != null) {
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
        };
    }

    /**
     * It is added as first so the last to execute
     */
    @Override
    protected ClientExecChain decorateMainExec(final ClientExecChain exec) {
        return new ClientExecChain() {
            @Override
            public CloseableHttpResponse execute(HttpRoute route, HttpRequestWrapper request,
                                                 HttpClientContext clientContext,
                                                 HttpExecutionAware execAware) throws IOException, HttpException {

                Span span = clientContext.getAttribute(ACTIVE_SPAN, Span.class);
                tracer.inject(span.context(), Format.Builtin.HTTP_HEADERS, new HttpHeadersInjectAdapter(request));
                return exec.execute(route, request, clientContext, execAware);
            }
        };
    }
}
