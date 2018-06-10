package com.akr.nifi.google.pubsub.consumer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

import com.google.pubsub.v1.ReceivedMessage;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;


import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;


@Tags({"google", "pubsub", "pub sub", "consumer", "renault", "khajour", "proxy"})

@CapabilityDescription("Google Pub Sub consumer")
public class NifiPubsubProcessor extends AbstractProcessor {


    public static final PropertyDescriptor authProperty = new PropertyDescriptor.Builder().name("Authentication Keys")
            .description("Required if outside of GCS. OAuth token (contents of myproject.json)")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor topicProperty = new PropertyDescriptor.Builder().name("Topic")
            .description("Name of topic")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor subProperty = new PropertyDescriptor.Builder().name("Subscription")
            .description("Name of the subscription")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor projectIdProperty = new PropertyDescriptor.Builder().name("Project ID")
            .description("Project ID")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor pullCountProperty = new PropertyDescriptor.Builder().name("Pull Count")
            .description("Number of messages to pull at request")
            .required(true)
            .defaultValue("10")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final PropertyDescriptor useProxy = new PropertyDescriptor.Builder()
            .name("Use Proxy")
            .description("Use Proxy")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();


    public static final PropertyDescriptor proxyHost = new PropertyDescriptor.Builder()
            .name("Proxy Host")
            .description("Proxy Host")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor proxyPort = new PropertyDescriptor.Builder()
            .name("Proxy Port")
            .description("Proxy Port")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor proxyUser = new PropertyDescriptor.Builder()
            .name("Proxy User")
            .description("Proxy User")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor proxyPassword = new PropertyDescriptor.Builder()
            .name("Proxy Password")
            .description("Proxy Password")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();


    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles received from Pubsub.")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;


    private PubsubConsumer pubsub = null;

    @Override
    protected void init(final ProcessorInitializationContext context) {

        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();

        descriptors.add(authProperty);
        descriptors.add(subProperty);
        descriptors.add(topicProperty);
        descriptors.add(projectIdProperty);
        descriptors.add(pullCountProperty);

        descriptors.add(useProxy);
        descriptors.add(proxyHost);
        descriptors.add(proxyPort);
        descriptors.add(proxyUser);
        descriptors.add(proxyPassword);


        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }


    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) {

        if (pubsub == null) {
            try {
                pubsub = new PubsubConsumer()
                        .project(context.getProperty(projectIdProperty).getValue())
                        .credentials(context.getProperty(authProperty).getValue())
                        .subscription(context.getProperty(subProperty).getValue())
                        .useProxy(Boolean.valueOf(context.getProperty(useProxy).getValue()))
                        .proxyHost(context.getProperty(proxyHost).getValue())
                        .proxyPort(context.getProperty(proxyPort).getValue())
                        .proxyUser(context.getProperty(proxyUser).getValue())
                        .proxyPassword(context.getProperty(proxyPassword).getValue())
                        .processor(this)
                        .build();
            } catch (Exception ex) {
                throw new ProcessException("Error configuring pubsub", ex);
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        try {
            List<ReceivedMessage> mess = pubsub.getMessages();
            Iterator<ReceivedMessage> messages = mess.iterator();

            while(messages.hasNext()) {
                final ReceivedMessage msg = messages.next();

                FlowFile flow = session.create();

                flow = session.write(flow, new OutputStreamCallback() {
                    @Override
                    public void process(OutputStream out) throws IOException {
                        out.write(msg.getMessage().getData().toByteArray());
                    }
                });

                flow = session.putAttribute(flow, "ack_id", msg.getAckId());
                flow = session.putAttribute(flow, "filename", Long.toString(System.nanoTime()));

                session.transfer(flow, REL_SUCCESS);
            }

            session.commit();

        } catch( Exception ex){
            throw new ProcessException("Error  getting pubsub messages", ex);
        }

    }

}