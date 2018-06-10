package com.akr.nifi.google.pubsub.consumer;


import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.*;

import java.util.*;

import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.cloud.ServiceOptions;

import com.google.api.client.http.HttpTransport;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HttpContext;

import com.google.auth.http.HttpTransportFactory;
import com.google.api.client.http.apache.ApacheHttpTransport;

import org.apache.http.message.BasicHeader;

import com.google.auth.oauth2.GoogleCredentials;


import java.net.Authenticator;
import java.net.PasswordAuthentication;

import com.google.auth.Credentials;


import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.conn.params.ConnRoutePNames;

import java.io.IOException;


import java.io.ByteArrayInputStream;


public class PubsubConsumer {

    private ProjectSubscriptionName subscriptionName;
    private String projectId;
    private String subscriptionId;
    private String pubsubCredentials;

    private boolean useProxy;
    private String proxyHost;
    private String proxyPort;
    private String proxyUser;
    private String proxyPassword;
    private GoogleCredentials proxyCredentials;
    private CredentialsProvider proxyCredentialsProvider;

    private NifiPubsubProcessor nifiProcessor;

    private ApacheHttpTransport mHttpTransport;

    public PubsubConsumer credentials(String creds) {
        this.pubsubCredentials = creds;
        return this;
    }

    public PubsubConsumer subscription(String subscription) {
        this.subscriptionId = subscription;
        return this;
    }

    public PubsubConsumer project(String project) {
        this.projectId = project;
        return this;
    }

    public PubsubConsumer processor(NifiPubsubProcessor processor) {
        this.nifiProcessor = processor;
        return this;
    }

    public PubsubConsumer useProxy(boolean useProxy) {
        this.useProxy = useProxy;
        return this;
    }

    public PubsubConsumer proxyHost(String proxyHost) {
        this.proxyHost = proxyHost;
        return this;
    }

    public PubsubConsumer proxyPort(String proxyPort) {
        this.proxyPort = proxyPort;
        return this;
    }

    public PubsubConsumer proxyUser(String proxyUser) {
        this.proxyUser = proxyUser;
        return this;
    }

    public PubsubConsumer proxyPassword(String proxyPassword) {
        this.proxyPassword = proxyPassword;
        return this;
    }

    public PubsubConsumer build() throws Exception {

        connect();
        return this;
    }



    public List<ReceivedMessage> getMessages() throws Exception {

        //CredentialsProvider credentialsProvider = FixedCredentialsProvider.create(ServiceAccountCredentials.fromStream(new ByteArrayInputStream(ServiceAccountCredentials.getBytes())));

        CredentialsProvider credentialsProvider = FixedCredentialsProvider.create(
                ServiceAccountCredentials.fromStream(new ByteArrayInputStream(pubsubCredentials.getBytes())));



        CredentialsProvider cp;

        if (useProxy) {
            cp = proxyCredentialsProvider;
        } else {
            cp = credentialsProvider;
        }

        SubscriberStubSettings subscriberStubSettings = SubscriberStubSettings.newBuilder()
                .setCredentialsProvider(cp)
                .build();


        SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings);

        String subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId);
        PullRequest pullRequest =
                PullRequest.newBuilder()
                        .setMaxMessages(10)
                        .setReturnImmediately(false)
                        .setSubscription(subscriptionName)
                        .build();


        PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);


        List<String> ackIds = new ArrayList<>();
        for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
            ackIds.add(message.getAckId());
        }

        AcknowledgeRequest acknowledgeRequest =
                AcknowledgeRequest.newBuilder()
                        .setSubscription(subscriptionName)
                        .addAllAckIds(ackIds)
                        .build();

        subscriber.acknowledgeCallable().call(acknowledgeRequest);

        return pullResponse.getReceivedMessagesList();


    }


    public void connect() {

        String projectId = ServiceOptions.getDefaultProjectId();

        System.setProperty("https.proxyHost", proxyHost);
        System.setProperty("https.proxyPort", proxyPort);

        try {
            Authenticator.setDefault(
                    new Authenticator() {
                        @Override
                        public PasswordAuthentication getPasswordAuthentication() {
                            return new PasswordAuthentication(
                                    proxyUser, proxyPassword.toCharArray());
                        }
                    }
            );

            HttpHost proxy = new HttpHost(proxyHost, Integer.parseInt(proxyPort));
            DefaultHttpClient httpClient = new DefaultHttpClient();
            httpClient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);

            Base64.Encoder base64 = Base64.getEncoder();


            final String basicAuth = base64.encodeToString((proxyUser + ":" + proxyPassword).getBytes());

            httpClient.addRequestInterceptor(new HttpRequestInterceptor() {
                @Override
                public void process(org.apache.http.HttpRequest request, HttpContext context) throws HttpException, IOException {
                    if (request.getRequestLine().getMethod().equals("CONNECT"))
                        request.addHeader(new BasicHeader("Proxy-Authorization", basicAuth));
                }
            });

            mHttpTransport = new ApacheHttpTransport(httpClient);

            HttpTransportFactory hf = new HttpTransportFactory() {
                @Override
                public HttpTransport create() {
                    return mHttpTransport;
                }
            };

            proxyCredentials = GoogleCredentials.getApplicationDefault(hf);

            proxyCredentialsProvider = new GoogleCredentialsProvider() {
                public List<String> getScopesToApply() {
                    return Arrays.asList("https://www.googleapis.com/auth/pubsub");
                }

                public Credentials getCredentials() {
                    return proxyCredentials;
                }

                public List<String> getJwtEnabledScopes() {
                    return Arrays.asList("https://www.googleapis.com/auth/pubsub");
                }
            };

        } catch (Exception ex) {
            System.out.println("ERROR " + ex);
        }
    }
}
