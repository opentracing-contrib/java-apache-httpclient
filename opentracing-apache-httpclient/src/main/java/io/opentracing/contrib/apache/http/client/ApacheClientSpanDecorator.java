package io.opentracing.contrib.apache.http.client;

import io.opentracing.Span;
import io.opentracing.tag.Tags;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpRequestWrapper;
import org.apache.http.protocol.HttpContext;

/**
 * Decorate span at different stages of request processing. Do not finish span in decorator.
 *
 * @author Pavol Loffay
 */
public interface ApacheClientSpanDecorator {

    /**
     * Decorate span before request is fired.
     *
     * @param request request
     * @param httpContext context
     * @param span span to decorate
     */
    void onRequest(HttpRequestWrapper request, HttpContext httpContext, Span span);

    /**
     * Decorate span after response is received.
     *
     * @param response response
     * @param httpContext context
     * @param span span to decorate
     */
    void onResponse(HttpResponse response, HttpContext httpContext, Span span);

    /**
     *  Decorate span span on error e.g. {@link java.net.UnknownHostException}/
     *
     * @param request request
     * @param httpContext context
     * @param ex exception
     * @param span span to decorate
     */
    void onError(HttpRequest request, HttpContext httpContext, Exception ex, Span span);

    /**
     * Decorator which adds standard set of tags and logs.
     */
    class StandardTags implements ApacheClientSpanDecorator {
        private static final Logger log = Logger.getLogger(StandardTags.class.getName());

        @Override
        public void onRequest(HttpRequestWrapper request, HttpContext httpContext, Span span) {
            Tags.HTTP_METHOD.set(span, request.getRequestLine().getMethod());

            URI uri = request.getURI();
            Tags.HTTP_URL.set(span, request.getRequestLine().getUri());
            Tags.PEER_PORT.set(span, uri.getPort() == -1 ? 80 : uri.getPort());
            Tags.PEER_HOSTNAME.set(span, uri.getHost());
        }

        @Override
        public void onResponse(HttpResponse response, HttpContext httpContext, Span span) {
            Tags.HTTP_STATUS.set(span, response.getStatusLine().getStatusCode());
        }

        @Override
        public void onError(HttpRequest request, HttpContext httpContext, Exception ex, Span span) {
            Tags.ERROR.set(span, Boolean.TRUE);

            Map<String, Object> errorLogs = new HashMap<>(2);
            errorLogs.put("event", Tags.ERROR.getKey());
            errorLogs.put("error.object", ex);
            span.log(errorLogs);
        }
    }
}
