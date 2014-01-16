package org.apache.activemq.leveldb.replicated.hazelcast;

import com.hazelcast.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SimpleTopicProtocol implements Protocol {
    private static final Logger log = LoggerFactory.getLogger(SimpleTopicProtocol.class);
    private static final String MANAGEMENT_TOPIC_NAME = "managementTopic";

    private ITopic<String> managementTopic;
    private Member localMember;
    private MessageProcessor messageProcessor;


    private static final String MASTER_ADDRESS_REQUEST = "MASTER_ADDRESS_REQUEST";
    private static final String MASTER_ADDRESS_REPLY = "MASTER_ADDRESS_REPLY";
    private static final String POSITION_REQUEST = "POSITION_REQUEST";
    private static final String POSITION_REPLY = "POSITION_REPLY";
    private static final String MASTER_ELECTED = "MASTER_ELECTED";
    private static final String NEW_MEMBER_CONNECTED = "NEW_MEMBER_CONNECTED";
    private static final String MEMBER_DISCONNECTED = "MEMBER_DISCONNECTED";


    @Override
    public void start(HazelcastInstance hazelcastInstance, MessageProcessor messageProcessor) {
        this.messageProcessor = messageProcessor;

        localMember = hazelcastInstance.getCluster().getLocalMember();
        managementTopic = hazelcastInstance.getTopic(MANAGEMENT_TOPIC_NAME);
        managementTopic.addMessageListener(new MessageListener<String>() {
            @Override
            public void onMessage(com.hazelcast.core.Message<String> message) {
                processIncomingMessage(message);
            }
        });
    }

    @Override
    public void stop() {
    }


    private void processIncomingMessage(Message<String> message) {
        if (shouldProcessMessage(message)) {
            log.info("Processing message {}...", message.getMessageObject());
            String messagePayload = message.getMessageObject();
            Member src = message.getPublishingMember();
            if (!isLocalMessage(message ) && messagePayload.startsWith(MASTER_ADDRESS_REPLY)) {
                messageProcessor.processMasterAddressReply(src, messagePayload.substring(MASTER_ADDRESS_REPLY.length()));
            } else if (messagePayload.startsWith(MASTER_ADDRESS_REQUEST)) {
                messageProcessor.processMasterAddressRequest(src, messagePayload.substring(MASTER_ADDRESS_REQUEST.length()));
            } else if (!isLocalMessage(message ) && messagePayload.startsWith(POSITION_REQUEST)) {
                messageProcessor.processPositionRequest(src, messagePayload.substring(POSITION_REQUEST.length()));
            } else if (!isLocalMessage(message ) && messagePayload.startsWith(POSITION_REPLY)) {
                String[] payloadArray = messagePayload.substring(POSITION_REPLY.length()).split(":");
                String receivedElectionId = payloadArray[0];
                long position = Long.parseLong(payloadArray[1]);
                messageProcessor.processPositionReply(src, receivedElectionId, position);
            } else if (!isLocalMessage(message ) && messagePayload.startsWith(MASTER_ELECTED)) {
                messageProcessor.processMasterElected(src, messagePayload.substring(MASTER_ELECTED.length()));
            } else if (!isLocalMessage(message ) && messagePayload.startsWith(NEW_MEMBER_CONNECTED)) {
                messageProcessor.processNewMemberConnected(src, messagePayload.substring(NEW_MEMBER_CONNECTED.length()));
            }
        }
    }

    @Override
    public void publishMasterAddress(String address) {
        managementTopic.publish(MASTER_ADDRESS_REPLY + address);
    }

    @Override
    public void publishState(String electionId, long position) {
        managementTopic.publish(POSITION_REPLY + electionId+":"+position);
    }

    @Override
    public void publishElected(String electedNode) {
        managementTopic.publish(MASTER_ELECTED + electedNode);
    }

    @Override
    public void publishPositionRequest(String electionId) {
        managementTopic.publish(POSITION_REQUEST+electionId);
    }

    @Override
    public void publishNewMemberConnected() {
        managementTopic.publish(NEW_MEMBER_CONNECTED);
    }

    @Override
    public void requestMasterAddress() {
        managementTopic.publish(MASTER_ADDRESS_REQUEST);
    }

    private boolean shouldProcessMessage(Message message) {
        return true;
    }

    private boolean isLocalMessage(Message<String> message) {
        return localMember.equals(message.getPublishingMember());
    }

}
