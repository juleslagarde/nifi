/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.grpc;

import io.grpc.Attributes;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * Simple gRPC service call interceptor that enforces various controls.
 * Despite its name, it does not contain any FlowFileIngestService specific logic.
 */
public class FlowFileIngestServiceInterceptor implements ServerInterceptor {

    public static final String DEFAULT_FOUND_SUBJECT = "none";
    private static final String UNKNOWN_IP = "unknown-ip";
    private static final String DN_UNAUTHORIZED = "The client DN does not have permission to send gRPC requests to this NiFi. ";
    private static final ServerCall.Listener IDENTITY_LISTENER = new ServerCall.Listener(){};

    public static final Context.Key<String> REMOTE_HOST_KEY = Context.key(GRPCAttributeNames.REMOTE_HOST);
    public static final Context.Key<String> REMOTE_DN_KEY = Context.key(GRPCAttributeNames.REMOTE_USER_DN);

    private final ComponentLog logger;
    private Pattern authorizedDNPattern;

    /**
     * Create an interceptor that applies various controls per request
     *
     * @param logger the {@link ComponentLog} for the ListenGRPC processor
     */
    public FlowFileIngestServiceInterceptor(final ComponentLog logger) {
        this.logger = requireNonNull(logger);
    }

    /**
     * Enforce that the requestor DN matches the provided pattern.
     *
     * @param authorizedDNPattern the pattern which DNs must match
     *
     * @return this
     */
    public FlowFileIngestServiceInterceptor enforceDNPattern(final Pattern authorizedDNPattern) {
        this.authorizedDNPattern = requireNonNull(authorizedDNPattern);
        return this;
    }

    /**
     * Intercept incoming and outgoing messages and enforce any necessary controls
     *
     * @param call the request message
     * @param headers the request metadata
     * @param next the next interceptor in the interceptor chain prior to the service implementation
     * @param <I> The message request type (e.g. ReqT)
     * @param <O> The message reply type (e.g. RespT)
     *
     * @return a listener for the incoming call.
     */
    @Override
    public <I, O> ServerCall.Listener<I> interceptCall(
            final ServerCall<I, O> call,
            final Metadata headers,
            final ServerCallHandler<I, O> next) {

        final Attributes attributes = call.getAttributes();
        final SocketAddress socketAddress = attributes.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
        final String clientIp = clientIp(socketAddress);
        String foundSubject = DEFAULT_FOUND_SUBJECT;

        // enforce that the DN on the client cert matches the configured pattern
        final SSLSession sslSession = attributes.get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
        if (this.authorizedDNPattern != null && sslSession != null) {
            try {
                final Certificate[] certs = sslSession.getPeerCertificates();
                if (certs != null && certs.length > 0) {
                    for (final Certificate cert : certs) {
                        final X509Certificate x509Cert = toX509Certificate(cert);
                        foundSubject = x509Cert.getSubjectX500Principal().getName();
                        if (authorizedDNPattern.matcher(foundSubject).matches()) {
                            break;
                        } else {
                            logger.warn("Rejecting transfer attempt from " + foundSubject + " because the DN is not authorized, host=" + clientIp);
                            call.close(Status.PERMISSION_DENIED.withDescription(DN_UNAUTHORIZED + foundSubject), headers);
                            return IDENTITY_LISTENER;
                        }
                    }
                }
            } catch (final SSLPeerUnverifiedException e) {
                logger.debug("Skipping DN authorization for request from {}", clientIp, e);
            }
        }
        // contextualize the DN and IP for use in the RPC implementation
        final Context context = Context.current()
                .withValue(REMOTE_HOST_KEY, clientIp)
                .withValue(REMOTE_DN_KEY, foundSubject);

        // if we got to this point, there were no errors, call the next interceptor in the chain
        return Contexts.interceptCall(context, call, headers, next);
    }

    /**
     * Grabs the client IP from the socket address pulled from the request metadata, or UNKNOWN
     * if it's not possible to determine.
     *
     * @param socketAddress the socket address pulled from the gRPC request
     * @return the client IP
     */
    private String clientIp(final SocketAddress socketAddress) {
        if (socketAddress == null) {
            return UNKNOWN_IP;
        }

        if (!(socketAddress instanceof InetSocketAddress)) {
            return socketAddress.toString();
        }

        final InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
        final String hostString = inetSocketAddress.getHostString();
        return hostString == null ? UNKNOWN_IP : hostString;
    }

    private X509Certificate toX509Certificate(final Certificate certificate) {
        if (certificate instanceof X509Certificate) {
            return (X509Certificate) certificate;
        } else {
            throw new ProcessException("Certificate is not an X.509 certificate. Certificate type: " + certificate.getClass());
        }
    }

}
