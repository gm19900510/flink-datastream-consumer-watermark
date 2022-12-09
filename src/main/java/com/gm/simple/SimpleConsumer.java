package com.gm.simple;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RPCHook;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumer.class);

    private static final String GROUP = "GID_SIMPLE_CONSUMER";
    private static final String TOPIC = "SOURCE_TOPIC";
    private static final String TAGS = "*";

    private static RPCHook getAclRPCHook() {
        final String accessKey = "${AccessKey}";
        final String secretKey = "${SecretKey}";
        return new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
    }

    public static void main(String[] args) {
        DefaultMQPushConsumer consumer =
                new DefaultMQPushConsumer(
                        GROUP);
        consumer.setNamesrvAddr("120.48.81.173:9876");

        // When using aliyun products, you need to set up channels
        //consumer.setAccessChannel(AccessChannel.CLOUD);

        try {
            consumer.subscribe(TOPIC, TAGS);
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.registerMessageListener(
                (MessageListenerConcurrently)
                        (msgs, context) -> {
                            for (MessageExt msg : msgs) {
                                System.out.printf(
                                        "%s %s %d %s\n",
                                        msg.getMsgId(),
                                        msg.getBrokerName(),
                                        msg.getQueueId(),
                                        new String(msg.getBody()));
                            }
                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                        });

        try {
            consumer.start();
        } catch (MQClientException e) {
            LOGGER.info("send message failed. {}", e.toString());
        }
    }
}