[![Build Status][ci-img]][ci] [![Released Version][maven-img]][maven]

# OpenTracing Apache HttpClient 

Apache HttpClient instrumentation.

## Configuration

```java
GlobalTracer.register(tracer);
HttpClient httpClient = new TracingHttpClientBuilder()
    .build();

//custom configuration
HttpClient httpClient = new TracingHttpClientBuilder(
        redirectStrategy, 
        redirectHandlingDisabled,
        GlobalTracer.get(),
        spanDecorators)
    .build();
```

## Linking to parent span
If parent span context is not passed created client spans will be in a new trace.

### SpanManager
```java
spanManager.activate(parentSpan); // called in the same thread as client.execute()
client.execute(new HttpGet("url"));
```

### Manually 
```java
BasicHttpContext basicHttpContext = new BasicHttpContext();
basicHttpContext.setAttribute(Constants.PARENT_CONTEXT, parentSpan.context());
client.execute(new HttpGet("url"), basicHttpContext);
```

## Development
```shell
./mvnw clean install
```

## Release
Follow instructions in [RELEASE](RELEASE.md)

   [ci-img]: https://travis-ci.org/opentracing-contrib/java-apache-httpclient.svg?branch=master
   [ci]: https://travis-ci.org/opentracing-contrib/java-apache-httpclient
   [maven-img]: https://img.shields.io/maven-central/v/io.opentracing.contrib/opentracing-apache-httpclient.svg?maxAge=2592000
   [maven]: http://search.maven.org/#search%7Cga%7C1%7Copentracing-apache-httpclient
