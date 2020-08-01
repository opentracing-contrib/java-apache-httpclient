package io.opentracing.contrib.apache.http.client;

import java.util.Iterator;
import java.util.Map;

import org.apache.http.HttpRequest;

import io.opentracing.propagation.TextMap;

/**
 * @author Pavol Loffay
 */
public class HttpHeadersInjectAdapter implements TextMap {

    private HttpRequest httpRequest;

    public HttpHeadersInjectAdapter(HttpRequest httpRequest) {
        this.httpRequest = httpRequest;
    }

    @Override
    public void put(String key, String value) {
        httpRequest.setHeader(key, value);
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        throw new UnsupportedOperationException("This class should be used only with tracer#inject()");
    }
}
