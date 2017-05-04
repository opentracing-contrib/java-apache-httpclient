package io.opentracing.contrib.apache;

import io.opentracing.Tracer;
import io.opentracing.contrib.apache.ApacheClientSpanDecorator.StandardTags;
import io.opentracing.util.GlobalTracer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.execchain.ClientExecChain;

/**
 * @author Pavol Loffay
 */
public class TracingHttpClientBuilder extends org.apache.http.impl.client.HttpClientBuilder {

    private final RedirectStrategy redirectStrategy;
    private final boolean redirectHandlingDisabled;

    private final Tracer tracer;
    private final List<ApacheClientSpanDecorator> spanDecorators;

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
        return new TracingClientExec(requestExecutor, redirectStrategy,
                redirectHandlingDisabled, tracer, spanDecorators);
    }
}
