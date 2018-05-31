package com.akr.nifi.google.pubsub.consumer;

import java.util.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;


import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;


@Tags({"google", "pubsub", "consumer", "renault"})

@CapabilityDescription("This example processor loads a resource from the nar and writes it to the FlowFile content")
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

        if ( "true".equals(context.getProperty(useProxy).getValue())) {

            System.setProperty("http.proxyHost", context.getProperty(proxyHost).getValue());
            System.setProperty("https.proxyHost", context.getProperty(proxyHost).getValue());

            System.setProperty("http.proxyPort", context.getProperty(proxyPort).getValue());
            System.setProperty("https.proxyPort", context.getProperty(proxyPort).getValue());

            System.setProperty("http.proxyUser", context.getProperty(proxyUser).getValue());
            System.setProperty("https.proxyUser", context.getProperty(proxyUser).getValue());

            System.setProperty("http.proxyPassword", context.getProperty(proxyPassword).getValue());
            System.setProperty("https.proxyPassword", context.getProperty(proxyPassword).getValue());

            //System.setProperty("GRPC_PROXY_EXP", "xxxx:3128");


/**
            Authenticator.setDefault(
                    new Authenticator() {
                        @Override
                        public PasswordAuthentication getPasswordAuthentication() {
                            return new PasswordAuthentication(
                                    context.getProperty(proxyUser).getValue(),
                                    context.getProperty(proxyPassword).getValue().toCharArray());
                        }
                    }
            );
**/

        }


        if (pubsub == null) {
            try {
                pubsub = new PubsubConsumer()
                        .project(context.getProperty(projectIdProperty).getValue())
                        .credentials(context.getProperty(authProperty).getValue())
                        .subscription(context.getProperty(subProperty).getValue())
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
            pubsub.getMessages();
        } catch( Exception ex){
            throw new ProcessException("Error  getting pubsub messages", ex);
        }

    }


    public void log(String msg){
        getLogger().info(msg);
    }



}