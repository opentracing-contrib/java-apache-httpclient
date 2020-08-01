package io.opentracing.contrib.apache.http.client;

import io.opentracing.Span;
import io.opentracing.tag.Tags;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpRequestWrapper;
import org.apache.http.protocol.HttpContext;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

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
            URI uri = request.getURI();
            HttpHost target = request.getTarget();

            Tags.HTTP_METHOD.set(span, request.getRequestLine().getMethod());

            if (uri != null) {
                Tags.HTTP_URL.set(span, uri.toString());
                Tags.PEER_HOSTNAME.set(span, uri.getHost());
                int port = uri.getPort();
                if (port == -1) {
                    String scheme = uri.getScheme();
                    if (scheme != null) {
                        port = scheme.equalsIgnoreCase("https") ? 443 : 80;
                    }
                }
                Tags.PEER_PORT.set(span, port);
            } else if (target != null) {
                Tags.HTTP_URL.set(span, request.getTarget() + request.getRequestLine().getUri());
                Tags.PEER_HOSTNAME.set(span, target.getHostName());
                Tags.PEER_PORT.set(span, target.getPort() == -1 ? target.getSchemeName().equalsIgnoreCase("https") ? 443 : 80 : target.getPort());
            }
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
