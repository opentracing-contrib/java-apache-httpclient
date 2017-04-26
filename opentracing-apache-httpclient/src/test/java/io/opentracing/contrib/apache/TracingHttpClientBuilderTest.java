package io.opentracing.contrib.apache;


import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.localserver.LocalServerTestBase;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.opentracing.Span;
import io.opentracing.contrib.spanmanager.DefaultSpanManager;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;

/**
 * @author Pavol Loffay
 */
public class TracingHttpClientBuilderTest extends LocalServerTestBase {

    private MockTracer mockTracer = new MockTracer(MockTracer.Propagator.TEXT_MAP);

    private HttpHost serverHost;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.clientBuilder = new TracingHttpClientBuilder(DefaultRedirectStrategy.INSTANCE, false, mockTracer,
                Collections.<ApacheClientSpanDecorator>singletonList(new ApacheClientSpanDecorator.StandardTags()));

        this.serverBootstrap.registerHandler(RedirectHandler.MAPPING, new RedirectHandler())
                .registerHandler(PropagationTestHandler.MAPPING, new PropagationTestHandler());
        this.serverHost = super.start();
    }

    @After
    public void shutDown() throws Exception {
        if(this.httpclient != null) {
            this.httpclient.close();
        }
        if(this.server != null) {
            this.server.shutdown(1L, TimeUnit.SECONDS);
        }
        mockTracer.reset();
    }

    @Test
    public void testStandardTags() throws IOException {
        {
            CloseableHttpClient client = clientBuilder.build();
            client.execute(new HttpGet(serverUrl("/echo/a")));
        }

        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(1, mockSpans.size());

        MockSpan mockSpan = mockSpans.get(0);
        Assert.assertEquals("GET", mockSpan.operationName());

        Assert.assertEquals(7, mockSpan.tags().size());
        Assert.assertEquals(Tags.SPAN_KIND_CLIENT, mockSpan.tags().get(Tags.SPAN_KIND.getKey()));
        Assert.assertNotNull(mockSpan.tags().get(Tags.COMPONENT.getKey()));
        Assert.assertEquals("GET", mockSpan.tags().get(Tags.HTTP_METHOD.getKey()));
        Assert.assertEquals(serverUrl("/echo/a"), mockSpan.tags().get(Tags.HTTP_URL.getKey()));
        Assert.assertEquals(200, mockSpan.tags().get(Tags.HTTP_STATUS.getKey()));
        Assert.assertEquals(serverHost.getPort(), mockSpan.tags().get(Tags.PEER_PORT.getKey()));
        Assert.assertEquals(serverHost.getHostName(), mockSpan.tags().get(Tags.PEER_HOSTNAME.getKey()));

        Assert.assertEquals(0, mockSpan.logEntries().size());
    }

    @Test
    public void testRedirect() throws URISyntaxException, IOException {
        {
            HttpClient client = clientBuilder.build();
            client.execute(new HttpGet(serverUrl(RedirectHandler.MAPPING)));
        }

        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(1, mockSpans.size());

        MockSpan mockSpan = mockSpans.get(0);
        Assert.assertEquals("GET", mockSpan.operationName());

        Assert.assertEquals(7, mockSpan.tags().size());
        Assert.assertEquals(Tags.SPAN_KIND_CLIENT, mockSpan.tags().get(Tags.SPAN_KIND.getKey()));
        Assert.assertNotNull(mockSpan.tags().get(Tags.COMPONENT.getKey()));
        Assert.assertEquals("GET", mockSpan.tags().get(Tags.HTTP_METHOD.getKey()));
        Assert.assertEquals(serverUrl("/redirect"), mockSpan.tags().get(Tags.HTTP_URL.getKey()));
        Assert.assertEquals(200, mockSpan.tags().get(Tags.HTTP_STATUS.getKey()));
        Assert.assertEquals(serverHost.getPort(), mockSpan.tags().get(Tags.PEER_PORT.getKey()));
        Assert.assertEquals(serverHost.getHostName(), mockSpan.tags().get(Tags.PEER_HOSTNAME.getKey()));

        Assert.assertEquals(1, mockSpan.logEntries().size());
        Assert.assertEquals(4, mockSpan.logEntries().get(0).fields().size());
        Assert.assertEquals("redirect", mockSpan.logEntries().get(0).fields().get("event"));
        Assert.assertEquals(serverHost.getPort(), mockSpan.logEntries().get(0).fields().get(Tags.PEER_PORT.getKey()));
        Assert.assertEquals(serverHost.getHostName(), mockSpan.logEntries().get(0).fields().get(Tags.PEER_HOSTNAME.getKey()));
        Assert.assertEquals("/echo/a", mockSpan.logEntries().get(0).fields().get("Location"));
    }

    @Test
    public void testDisableRedirectHandling() throws URISyntaxException, IOException {
        {
            HttpClient client = new TracingHttpClientBuilder(DefaultRedirectStrategy.INSTANCE, true, mockTracer,
                    Collections.<ApacheClientSpanDecorator>singletonList(new ApacheClientSpanDecorator.StandardTags()))
                    .build();

            client.execute(new HttpGet(serverUrl(RedirectHandler.MAPPING)));
        }

        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(1, mockSpans.size());

        MockSpan mockSpan = mockSpans.get(0);
        Assert.assertEquals("GET", mockSpan.operationName());

        Assert.assertEquals(7, mockSpan.tags().size());
        Assert.assertEquals(Tags.SPAN_KIND_CLIENT, mockSpan.tags().get(Tags.SPAN_KIND.getKey()));
        Assert.assertNotNull(mockSpan.tags().get(Tags.COMPONENT.getKey()));
        Assert.assertEquals("GET", mockSpan.tags().get(Tags.HTTP_METHOD.getKey()));
        Assert.assertEquals(serverUrl("/redirect"), mockSpan.tags().get(Tags.HTTP_URL.getKey()));
        Assert.assertEquals(301, mockSpan.tags().get(Tags.HTTP_STATUS.getKey()));
        Assert.assertEquals(serverHost.getPort(), mockSpan.tags().get(Tags.PEER_PORT.getKey()));
        Assert.assertEquals(serverHost.getHostName(), mockSpan.tags().get(Tags.PEER_HOSTNAME.getKey()));

        Assert.assertEquals(0, mockSpan.logEntries().size());
    }

    @Test
    public void testRequestConfigDisabledRedirects() throws URISyntaxException, IOException {
        {
            HttpClient client = clientBuilder
                    .setDefaultRequestConfig(RequestConfig.custom()
                                .setRedirectsEnabled(false)
                                .build())
                    .build();
            client.execute(new HttpGet(serverUrl(RedirectHandler.MAPPING)));
        }

        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(1, mockSpans.size());

        MockSpan mockSpan = mockSpans.get(0);
        Assert.assertEquals("GET", mockSpan.operationName());

        Assert.assertEquals(7, mockSpan.tags().size());
        Assert.assertEquals(Tags.SPAN_KIND_CLIENT, mockSpan.tags().get(Tags.SPAN_KIND.getKey()));
        Assert.assertNotNull(mockSpan.tags().get(Tags.COMPONENT.getKey()));
        Assert.assertEquals("GET", mockSpan.tags().get(Tags.HTTP_METHOD.getKey()));
        Assert.assertEquals(serverUrl("/redirect"), mockSpan.tags().get(Tags.HTTP_URL.getKey()));
        Assert.assertEquals(301, mockSpan.tags().get(Tags.HTTP_STATUS.getKey()));
        Assert.assertEquals(serverHost.getPort(), mockSpan.tags().get(Tags.PEER_PORT.getKey()));
        Assert.assertEquals(serverHost.getHostName(), mockSpan.tags().get(Tags.PEER_HOSTNAME.getKey()));

        Assert.assertEquals(0, mockSpan.logEntries().size());
    }

    @Test
    public void testParentSpan() throws IOException {
        {
            Span parentSpan = mockTracer.buildSpan("parent")
                    .start();

            DefaultSpanManager.getInstance().activate(parentSpan);

            CloseableHttpClient client = clientBuilder.build();
            BasicHttpContext basicHttpContext = new BasicHttpContext();

            client.execute(new HttpGet(serverUrl("/echo/a")), basicHttpContext);

            parentSpan.finish();
        }

        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(2, mockSpans.size());

        Assert.assertEquals(mockSpans.get(0).context().traceId(), mockSpans.get(1).context().traceId());
        Assert.assertEquals(mockSpans.get(0).parentId(), mockSpans.get(1).context().spanId());
    }

    @Test
    public void testPropagation() throws IOException {
        {
            HttpClient client = clientBuilder
                    .setDefaultRequestConfig(RequestConfig.custom()
                            .setRedirectsEnabled(false)
                            .build())
                    .build();
            client.execute(new HttpGet(serverUrl(PropagationTestHandler.MAPPING)));
        }

        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(1, mockSpans.size());

        MockSpan mockSpan = mockSpans.get(0);
        Assert.assertEquals(PropagationTestHandler.lastRequest.getFirstHeader("traceId").getValue(),
                String.valueOf(mockSpan.context().traceId()));
        Assert.assertEquals(PropagationTestHandler.lastRequest.getFirstHeader("spanId").getValue(),
                String.valueOf(mockSpan.context().spanId()));
    }

    @Test
    public void testUnknownHostException() throws IOException {
        CloseableHttpClient client = clientBuilder.build();

        try {
            client.execute(new HttpGet("http://notexisting.example.com"));
        } catch (UnknownHostException ex) {
        }

        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(1, mockSpans.size());

        MockSpan mockSpan = mockSpans.get(0);
        Assert.assertEquals(Boolean.TRUE, mockSpan.tags().get(Tags.ERROR.getKey()));

        // logs
        Assert.assertEquals(1, mockSpan.logEntries().size());
        Assert.assertEquals(2, mockSpan.logEntries().get(0).fields().size());
        Assert.assertEquals(Tags.ERROR.getKey(), mockSpan.logEntries().get(0).fields().get("event"));
        Assert.assertNotNull(mockSpan.logEntries().get(0).fields().get("error.object"));
    }

    protected String serverUrl(String path) {
        return serverHost.toString() + path;
    }

    public static class RedirectHandler implements HttpRequestHandler {

        public static final String MAPPING = "/redirect";
        public static final String REDIRECT_LOCATION = "/echo/a";

        @Override
        public void handle(HttpRequest request, HttpResponse response, HttpContext context)
                throws HttpException, IOException {

            response.setStatusCode(HttpStatus.SC_MOVED_PERMANENTLY);
            response.addHeader("Location", REDIRECT_LOCATION);
        }
    }

    public static class PropagationTestHandler implements HttpRequestHandler {
        public static final String MAPPING = "/propagation";
        public static HttpRequest lastRequest;

        @Override
        public void handle(HttpRequest request, HttpResponse response, HttpContext context)
                throws HttpException, IOException {
            // TODO this is ugly...
            lastRequest = request;
            response.setStatusCode(HttpStatus.SC_OK);
        }
    }
}
