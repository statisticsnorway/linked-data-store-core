package no.ssb.lds.core.controller;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import io.undertow.util.Methods;
import io.undertow.util.StatusCodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A handler that implements https://www.w3.org/TR/cors/#resource-processing-model.
 */
public class CORSHandler implements HttpHandler {

    public static final HttpString ACCESS_CONTROL_ALLOW_CREDENTIALS = new HttpString("Access-Control-Allow-Credentials");
    public static final HttpString ACCESS_CONTROL_ALLOW_HEADERS = new HttpString("Access-Control-Allow-Headers");
    public static final HttpString ACCESS_CONTROL_ALLOW_METHODS = new HttpString("Access-Control-Allow-Methods");
    public static final HttpString ACCESS_CONTROL_ALLOW_ORIGIN = new HttpString("Access-Control-Allow-Origin");
    public static final HttpString ACCESS_CONTROL_EXPOSE_HEADERS = new HttpString("Access-Control-Expose-Headers");
    public static final HttpString ACCESS_CONTROL_MAX_AGE = new HttpString("Access-Control-Max-Age");
    public static final Set<HttpString> DEFAULT_ALLOW_HEADERS = Set.of(
            Headers.AUTHORIZATION, Headers.CONTENT_TYPE, Headers.RANGE);
    private static final Logger log = LoggerFactory.getLogger(CORSHandler.class);
    private static final HttpString ACCESS_CONTROL_REQUEST_METHOD = new HttpString("Access-Control-Request-Method");
    private static final HttpString ACCESS_CONTROL_REQUEST_HEADERS = new HttpString("Access-Control-Request-Headers");
    // Max age.
    private static final long DEFAULT_MAX_AGE = 864000; // 10 days
    private static final Set<HttpString> DEFAULT_ALLOW_METHODS = Set.of(
            Methods.DELETE, Methods.GET, Methods.HEAD, Methods.OPTIONS, Methods.PATCH, Methods.POST, Methods.PUT);
    // Response code returned upon preflight.
    private static int DEFAULT_PREFLIGHT = StatusCodes.NO_CONTENT;
    private final HttpHandler next;
    private final HttpHandler fail;
    private final List<Pattern> originPatterns;
    private final int preflightResponseCode;
    private final long maxAge;
    private final Set<String> allowedMethods;
    private final Set<String> allowedHeaders;

    private final boolean supportsCredential = false;

    public CORSHandler(HttpHandler next, List<Pattern> originPatterns) {
        this(next, next, originPatterns);
    }

    public CORSHandler(HttpHandler next, HttpHandler fail, List<Pattern> originPatterns) {
        this(next, fail, originPatterns, DEFAULT_PREFLIGHT, DEFAULT_MAX_AGE,
                DEFAULT_ALLOW_METHODS.stream().map(HttpString::toString).collect(Collectors.toSet()),
                DEFAULT_ALLOW_HEADERS.stream().map(HttpString::toString).collect(Collectors.toSet())
        );
    }

    public CORSHandler(HttpHandler next, List<Pattern> originPatterns, int preflightResponseCode, long maxAge,
                       Set<String> allowedMethods, Set<String> allowedHeaders) {
        this(next, next, originPatterns, preflightResponseCode, maxAge, allowedMethods, allowedHeaders);
    }

    public CORSHandler(HttpHandler next, HttpHandler fail, List<Pattern> originPatterns, int preflightResponseCode, long maxAge,
                       Set<String> allowedMethods, Set<String> allowedHeaders) {
        this.next = next;
        this.fail = fail;
        this.originPatterns = originPatterns;
        this.preflightResponseCode = preflightResponseCode;
        this.maxAge = maxAge;
        this.allowedMethods = allowedMethods;
        this.allowedHeaders = allowedHeaders;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        // See https://www.w3.org/TR/cors/#resource-processing-model
        try {
            hasOrigin(exchange);
            if (isOption(exchange)) {
                log.debug("Preflight check: {}", exchange);
                handlePreflight(exchange);
            } else {
                log.debug("Simple check: {}", exchange);
                String origin = checkOrigin(exchange);
                addAllowOrigin(exchange, origin);
                addExposeHeaders(exchange);
                next.handleRequest(exchange);
            }
        } catch (OutOfScopeException oose) {
            log.debug("Request out of scope: {}", oose.getMessage());
            fail.handleRequest(exchange);
        }
    }

    /**
     * If the Origin header is not present terminate this set of steps.
     * The request is outside the scope of this specification.
     */
    private void hasOrigin(HttpServerExchange exchange) throws OutOfScopeException {
        HeaderValues origin = exchange.getRequestHeaders().get(Headers.ORIGIN);
        if (origin == null) {
            throw new OutOfScopeException("no origin header");
        } else if (origin.isEmpty()) {
            throw new OutOfScopeException("empty origin header");
        }
    }

    private void handlePreflight(HttpServerExchange exchange) throws OutOfScopeException {
        String origin = checkOrigin(exchange);
        checkMethod(exchange);
        checkHeaders(exchange);

        addAllowOrigin(exchange, origin);
        addMaxAge(exchange);
        addAllowMethods(exchange);
        addAllowHeaders(exchange);
        exchange.setStatusCode(preflightResponseCode);
    }

    private void checkHeaders(HttpServerExchange exchange) throws OutOfScopeException {
        // If there are no Access-Control-Request-Headers headers let header field-names be the empty list.
        List<String> requestHeaders = getHeaderOrEmpty(exchange, ACCESS_CONTROL_REQUEST_HEADERS);
        // If any of the header field-names is not a ASCII case-insensitive match for any of the values in
        // list of headers do not set any additional headers and terminate this set of steps.
        boolean isMatch = false;
        for (String requestHeader : requestHeaders) {
            for (String allowedHeader : allowedHeaders) {
                if (requestHeader.equalsIgnoreCase(allowedHeader)) {
                    isMatch = true;
                }
            }
            if (!isMatch) {
                throw new OutOfScopeException("no header match");
            }
            isMatch = false;
        }
    }

    /**
     * If there is no Access-Control-Request-Method header or if parsing failed, do not set any additional headers
     * and terminate this set of steps. The request is outside the scope of this specification.
     */
    private void checkMethod(HttpServerExchange exchange) throws OutOfScopeException {
        String method = getOneHeaderOrFail(exchange, ACCESS_CONTROL_REQUEST_METHOD);
        if (!allowedMethods.contains(method)) {
            throw new OutOfScopeException("methods did not match");
        }
    }

    private HeaderValues getHeader(HttpServerExchange exchange, HttpString header) {
        return exchange.getRequestHeaders().get(header);
    }

    private String getOneHeaderOrFail(HttpServerExchange exchange, HttpString header) throws OutOfScopeException {
        HeaderValues headers = getHeaderOrFail(exchange, header);
        if (headers.size() > 1) {
            throw new OutOfScopeException("more than one " + header + " value");
        }
        return headers.getFirst();
    }

    private HeaderValues getHeaderOrFail(HttpServerExchange exchange, HttpString header) throws OutOfScopeException {
        HeaderValues headers = getHeader(exchange, header);
        if (headers == null) {
            throw new OutOfScopeException("no " + header + " in request");
        }
        return headers;
    }

    private List<String> getHeaderOrEmpty(HttpServerExchange exchange, HttpString header) throws OutOfScopeException {
        HeaderValues headers = getHeader(exchange, header);
        return headers == null ? Collections.emptyList() : headers;
    }

    /**
     * If the value of the Origin header is not a case-sensitive match for any of the values in list of
     * origins do not set any additional headers and terminate this set of steps.
     */
    private String checkOrigin(HttpServerExchange exchange) throws OutOfScopeException {
        String origin = getOneHeaderOrFail(exchange, Headers.ORIGIN);
        boolean originMatches = false;
        for (Pattern pattern : originPatterns) {
            if (pattern.matcher(origin).matches()) {
                originMatches = true;
                break;
            }
        }
        if (!originMatches) {
            throw new OutOfScopeException("origin " + origin + " did not match");
        }
        return origin;
    }

    private boolean isOption(HttpServerExchange exchange) {
        return Methods.OPTIONS.equals(exchange.getRequestMethod());
    }

    private void addMaxAge(HttpServerExchange exchange) {
        exchange.getResponseHeaders().put(ACCESS_CONTROL_MAX_AGE, maxAge);
    }

    private void addAllowHeaders(HttpServerExchange exchange) {
        exchange.getResponseHeaders().add(ACCESS_CONTROL_ALLOW_HEADERS, String.join(",", allowedHeaders));
    }

    private void addExposeHeaders(HttpServerExchange exchange) {
        exchange.getResponseHeaders().add(ACCESS_CONTROL_EXPOSE_HEADERS, String.join(",", allowedHeaders));
    }

    private void addAllowMethods(HttpServerExchange exchange) {
        exchange.getResponseHeaders().add(ACCESS_CONTROL_ALLOW_METHODS, String.join(",", allowedMethods));
    }

    //  The Access-Control-Allow-Origin header should contain the value that was sent in the request's Origin header.
    private void addAllowOrigin(HttpServerExchange exchange, String origin) {
        if (supportsCredential) {
            exchange.getResponseHeaders().add(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
        }
        exchange.getResponseHeaders().add(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        exchange.getResponseHeaders().add(Headers.VARY, Headers.ORIGIN_STRING);
    }

    private static final class OutOfScopeException extends Exception {
        public OutOfScopeException(String message) {
            super(message);
        }
    }


}
