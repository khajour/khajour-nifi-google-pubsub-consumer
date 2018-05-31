package com.akr.nifi.google.pubsub.consumer;


import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.*;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

import static com.google.pubsub.v1.ProjectSubscriptionName.of;


public class PubsubConsumer {

    private ProjectSubscriptionName subscriptionName;
    private String projectId;
    private String subscriptionId;
    private String credentials;
    private NifiPubsubProcessor nifiProcessor;



    public PubsubConsumer credentials(String creds) {

        this.credentials = creds;
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


    public PubsubConsumer build() throws Exception {


        return this;
    }

    public List<ReceivedMessage> getMessages() throws Exception {

        CredentialsProvider credentialsProvider = FixedCredentialsProvider.create(
                ServiceAccountCredentials.fromStream(new ByteArrayInputStream(credentials.getBytes())));


        SubscriberStubSettings subscriberStubSettings = SubscriberStubSettings.newBuilder()
                .setCredentialsProvider(credentialsProvider)
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


                nifiProcessor.log(message.getMessage().getData().toStringUtf8());

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

}
