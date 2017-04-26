package io.opentracing.contrib.apache;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpRequestWrapper;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;

import io.opentracing.Span;
import io.opentracing.tag.Tags;

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
     * Decorate span after redirect.
     *
     * @param response response
     * @param httpContext context
     * @param span span to decorate
     */
    void onRedirect(HttpResponse response, HttpContext httpContext, Span span);

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
            Tags.COMPONENT.set(span, "apache-httpclient");
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
        public void onRedirect(HttpResponse response, HttpContext httpContext, Span span) {
            Map<String, Object> redirectLogs = new HashMap<>(4);
            redirectLogs.put("event", "redirect");

            Header locationHeader = response.getFirstHeader("Location");
            if (locationHeader != null) {
                redirectLogs.put("Location", locationHeader.getValue());
            }

            URI uri = getURI(httpContext);
            if (uri != null) {
                redirectLogs.put(Tags.PEER_PORT.getKey(), uri.getPort() == -1 ? 80 : uri.getPort());
                redirectLogs.put(Tags.PEER_HOSTNAME.getKey(), uri.getHost());
            }

            span.log(redirectLogs);
        }

        @Override
        public void onError(HttpRequest request, HttpContext httpContext, Exception ex, Span span) {
            Tags.ERROR.set(span, Boolean.TRUE);

            Map<String, Object> errorLogs = new HashMap<>(2);
            errorLogs.put("event", Tags.ERROR.getKey());
            errorLogs.put("error.object", ex);
            span.log(errorLogs);
        }

        public static URI getURI(HttpContext httpContext) {
            HttpRequest httpRequest = (HttpRequest) httpContext.getAttribute(HttpCoreContext.HTTP_REQUEST);
            HttpHost httpHost = (HttpHost) httpContext.getAttribute(HttpCoreContext.HTTP_TARGET_HOST);

            try {
                return new URI(httpHost.toURI() + httpRequest.getRequestLine().getUri());
            } catch (URISyntaxException e) {
                log.severe("Could not construct URI some tags/logs will not be applied!");
                return null;
            }
        }
    }
}
