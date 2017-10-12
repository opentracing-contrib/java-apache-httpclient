package io.opentracing.contrib.apache.http.client;


import io.opentracing.Scope;
import io.opentracing.util.ThreadLocalScopeManager;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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

import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;

/**
 * @author Pavol Loffay
 */
public class TracingHttpClientBuilderTest extends LocalServerTestBase {

    private static MockTracer mockTracer = new MockTracer(new ThreadLocalScopeManager(), MockTracer.Propagator.TEXT_MAP);

    private HttpHost serverHost;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.clientBuilder = new TracingHttpClientBuilder(DefaultRedirectStrategy.INSTANCE, false, mockTracer,
                Collections.<ApacheClientSpanDecorator>singletonList(new ApacheClientSpanDecorator.StandardTags()));

        this.serverBootstrap.registerHandler(RedirectHandler.MAPPING, new RedirectHandler())
                .registerHandler(PropagationHandler.MAPPING, new PropagationHandler());
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
        Assert.assertEquals(2, mockSpans.size());

        MockSpan mockSpan = mockSpans.get(0);
        Assert.assertEquals("GET", mockSpan.operationName());

        Assert.assertEquals(6, mockSpan.tags().size());
        Assert.assertEquals(Tags.SPAN_KIND_CLIENT, mockSpan.tags().get(Tags.SPAN_KIND.getKey()));
        Assert.assertEquals("GET", mockSpan.tags().get(Tags.HTTP_METHOD.getKey()));
        Assert.assertEquals(serverUrl("/echo/a"), mockSpan.tags().get(Tags.HTTP_URL.getKey()));
        Assert.assertEquals(200, mockSpan.tags().get(Tags.HTTP_STATUS.getKey()));
        Assert.assertEquals(serverHost.getPort(), mockSpan.tags().get(Tags.PEER_PORT.getKey()));
        Assert.assertEquals(serverHost.getHostName(), mockSpan.tags().get(Tags.PEER_HOSTNAME.getKey()));
        Assert.assertEquals(0, mockSpan.logEntries().size());

        assertLocalSpan(mockSpans.get(1));
    }

    @Test
    public void testRedirect() throws URISyntaxException, IOException {
        {
            HttpClient client = clientBuilder.build();
            client.execute(new HttpGet(serverUrl(RedirectHandler.MAPPING)));
        }

        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(3, mockSpans.size());

        MockSpan mockSpan = mockSpans.get(0);
        Assert.assertEquals("GET", mockSpan.operationName());
        Assert.assertEquals(6, mockSpan.tags().size());
        Assert.assertEquals(Tags.SPAN_KIND_CLIENT, mockSpan.tags().get(Tags.SPAN_KIND.getKey()));
        Assert.assertEquals("GET", mockSpan.tags().get(Tags.HTTP_METHOD.getKey()));
        Assert.assertEquals(serverUrl("/redirect"), mockSpan.tags().get(Tags.HTTP_URL.getKey()));
        Assert.assertEquals(301, mockSpan.tags().get(Tags.HTTP_STATUS.getKey()));
        Assert.assertEquals(serverHost.getPort(), mockSpan.tags().get(Tags.PEER_PORT.getKey()));
        Assert.assertEquals(serverHost.getHostName(), mockSpan.tags().get(Tags.PEER_HOSTNAME.getKey()));

        mockSpan = mockSpans.get(1);
        Assert.assertEquals("GET", mockSpan.operationName());
        Assert.assertEquals(6, mockSpan.tags().size());
        Assert.assertEquals(Tags.SPAN_KIND_CLIENT, mockSpan.tags().get(Tags.SPAN_KIND.getKey()));
        Assert.assertEquals("GET", mockSpan.tags().get(Tags.HTTP_METHOD.getKey()));
        Assert.assertEquals(serverUrl("/propagation"), mockSpan.tags().get(Tags.HTTP_URL.getKey()));
        Assert.assertEquals(200, mockSpan.tags().get(Tags.HTTP_STATUS.getKey()));
        Assert.assertEquals(serverHost.getPort(), mockSpan.tags().get(Tags.PEER_PORT.getKey()));
        Assert.assertEquals(serverHost.getHostName(), mockSpan.tags().get(Tags.PEER_HOSTNAME.getKey()));

        assertLocalSpan(mockSpans.get(2));
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
        Assert.assertEquals(2, mockSpans.size());

        MockSpan mockSpan = mockSpans.get(0);
        Assert.assertEquals("GET", mockSpan.operationName());

        Assert.assertEquals(6, mockSpan.tags().size());
        Assert.assertEquals(Tags.SPAN_KIND_CLIENT, mockSpan.tags().get(Tags.SPAN_KIND.getKey()));
        Assert.assertEquals("GET", mockSpan.tags().get(Tags.HTTP_METHOD.getKey()));
        Assert.assertEquals(serverUrl("/redirect"), mockSpan.tags().get(Tags.HTTP_URL.getKey()));
        Assert.assertEquals(301, mockSpan.tags().get(Tags.HTTP_STATUS.getKey()));
        Assert.assertEquals(serverHost.getPort(), mockSpan.tags().get(Tags.PEER_PORT.getKey()));
        Assert.assertEquals(serverHost.getHostName(), mockSpan.tags().get(Tags.PEER_HOSTNAME.getKey()));
        Assert.assertEquals(0, mockSpan.logEntries().size());

        assertLocalSpan(mockSpans.get(1));
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
        Assert.assertEquals(2, mockSpans.size());

        MockSpan mockSpan = mockSpans.get(0);
        Assert.assertEquals("GET", mockSpan.operationName());

        Assert.assertEquals(6, mockSpan.tags().size());
        Assert.assertEquals(Tags.SPAN_KIND_CLIENT, mockSpan.tags().get(Tags.SPAN_KIND.getKey()));
        Assert.assertEquals("GET", mockSpan.tags().get(Tags.HTTP_METHOD.getKey()));
        Assert.assertEquals(serverUrl("/redirect"), mockSpan.tags().get(Tags.HTTP_URL.getKey()));
        Assert.assertEquals(301, mockSpan.tags().get(Tags.HTTP_STATUS.getKey()));
        Assert.assertEquals(serverHost.getPort(), mockSpan.tags().get(Tags.PEER_PORT.getKey()));
        Assert.assertEquals(serverHost.getHostName(), mockSpan.tags().get(Tags.PEER_HOSTNAME.getKey()));
        Assert.assertEquals(0, mockSpan.logEntries().size());

        assertLocalSpan(mockSpans.get(1));
    }

    @Test
    public void testActiveParentSpan() throws IOException {
        {
            Scope parentSpan = mockTracer.buildSpan("parent")
                    .startActive(true);

            CloseableHttpClient client = clientBuilder.build();
            client.execute(new HttpGet(serverUrl("/echo/a")));

            parentSpan.close();
        }

        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(3, mockSpans.size());

        Assert.assertEquals(mockSpans.get(0).context().traceId(), mockSpans.get(1).context().traceId());
        Assert.assertEquals(mockSpans.get(0).parentId(), mockSpans.get(1).context().spanId());

        assertLocalSpan(mockSpans.get(1));
    }

    @Test
    public void testManualParentSpan() throws IOException {
        MockSpan parent = mockTracer.buildSpan("parent")
                .startManual();

        {
            Scope parentSpan = mockTracer.buildSpan("parent")
                    .startActive(false);

            HttpContext context = new BasicHttpContext();
            context.setAttribute(Constants.PARENT_CONTEXT, parent.context());

            CloseableHttpClient client = clientBuilder.build();
            client.execute(new HttpGet(serverUrl("/echo/a")), context);
        }

        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(2, mockSpans.size());

        Assert.assertEquals(parent.context().traceId(), mockSpans.get(1).context().traceId());
        Assert.assertEquals(parent.context().spanId(), mockSpans.get(1).parentId());

        assertLocalSpan(mockSpans.get(1));
    }

    @Test
    public void testPropagationAfterRedirect() throws IOException {
        {
            HttpClient client = clientBuilder.build();
            client.execute(new HttpGet(serverUrl(RedirectHandler.MAPPING)));
        }

        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(3, mockSpans.size());

        // the last one is for redirect
        MockSpan mockSpan = mockSpans.get(1);
        Assert.assertEquals(PropagationHandler.lastRequest.getFirstHeader("traceId").getValue(),
                String.valueOf(mockSpan.context().traceId()));
        Assert.assertEquals(PropagationHandler.lastRequest.getFirstHeader("spanId").getValue(),
                String.valueOf(mockSpan.context().spanId()));

        assertLocalSpan(mockSpans.get(2));
    }

    @Test
    public void testUnknownHostException() throws IOException {
        CloseableHttpClient client = clientBuilder.build();

        try {
            client.execute(new HttpGet("http://notexisting.example.com"));
        } catch (UnknownHostException ex) {
        }

        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        Assert.assertEquals(2, mockSpans.size());

        MockSpan mockSpan = mockSpans.get(0);
        Assert.assertEquals(Boolean.TRUE, mockSpan.tags().get(Tags.ERROR.getKey()));

        // logs
        Assert.assertEquals(1, mockSpan.logEntries().size());
        Assert.assertEquals(2, mockSpan.logEntries().get(0).fields().size());
        Assert.assertEquals(Tags.ERROR.getKey(), mockSpan.logEntries().get(0).fields().get("event"));
        Assert.assertNotNull(mockSpan.logEntries().get(0).fields().get("error.object"));
    }

    @Test
    public void testMultipleSimultaneousRequests() throws ExecutionException, InterruptedException {
        int numberOfCalls = 100;

        Map<Long, MockSpan> parentSpans = new LinkedHashMap<>(numberOfCalls);

        ExecutorService executorService = Executors.newFixedThreadPool(20);
        List<Future<?>> futures = new ArrayList<>(numberOfCalls);
        for (int i = 0; i < numberOfCalls; i++) {

            final String requestUrl = serverUrl("/echo/a");

            final MockSpan parentSpan = mockTracer.buildSpan("foo")
                    .ignoreActiveSpan().start();
            parentSpan.setTag("request-url", requestUrl);
            parentSpans.put(parentSpan.context().spanId(), parentSpan);

            futures.add(executorService.submit(new Runnable() {
                @Override
                public void run() {
                    Scope activeParent = mockTracer.scopeManager().activate(parentSpan);
                    try {
                        httpclient.execute(new HttpGet(requestUrl));
                    } catch (IOException e) {
                        e.printStackTrace();
                        Assert.fail();
                    }
                }
            }));
        }

        // wait to finish all calls
        for (Future<?> future: futures) {
            future.get();
        }

        executorService.awaitTermination(1, TimeUnit.SECONDS);
        executorService.shutdown();

        List<MockSpan> mockSpans = mockTracer.finishedSpans();
        assertOnErrors(mockSpans);
        Assert.assertEquals(numberOfCalls * 2, mockSpans.size()); //local + network span

        // parentId - span
        Map<Long, MockSpan> map = new HashMap<>(mockSpans.size());
        for (MockSpan mockSpan: mockSpans) {
            map.put(mockSpan.context().spanId(), mockSpan);
        }

        for (MockSpan networkSpan: mockSpans) {
            if (networkSpan.tags().containsKey(Tags.COMPONENT.getKey())) {
                continue;
            }

            MockSpan localSpan = map.get(networkSpan.parentId());
            MockSpan parentBeforeClientSpan = parentSpans.get(localSpan.parentId());

            Assert.assertEquals(parentBeforeClientSpan.tags().get("request-url"), networkSpan.tags().get(Tags.HTTP_URL.getKey()));

            Assert.assertEquals(parentBeforeClientSpan.context().traceId(), localSpan.context().traceId());
            Assert.assertEquals(parentBeforeClientSpan.context().traceId(), networkSpan.context().traceId());
            Assert.assertEquals(parentBeforeClientSpan.context().spanId(), localSpan.parentId());
            Assert.assertEquals(0, networkSpan.generatedErrors().size());
            Assert.assertEquals(0, parentBeforeClientSpan.generatedErrors().size());
        }
    }

    public void assertLocalSpan(MockSpan mockSpan) {
        Assert.assertEquals(1, mockSpan.tags().size());
        Assert.assertEquals(TracingClientExec.COMPONENT_NAME, mockSpan.tags().get(Tags.COMPONENT.getKey()));
    }

    protected String serverUrl(String path) {
        return serverHost.toString() + path;
    }

    public static void assertOnErrors(List<MockSpan> spans) {
        for (MockSpan mockSpan: spans) {
            Assert.assertEquals(mockSpan.generatedErrors().toString(), 0, mockSpan.generatedErrors().size());
        }
    }

    public static class RedirectHandler implements HttpRequestHandler {

        public static final String MAPPING = "/redirect";

        @Override
        public void handle(HttpRequest request, HttpResponse response, HttpContext context)
                throws HttpException, IOException {

            response.setStatusCode(HttpStatus.SC_MOVED_PERMANENTLY);
            response.addHeader("Location", PropagationHandler.MAPPING);
        }
    }

    public static class PropagationHandler implements HttpRequestHandler {
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
