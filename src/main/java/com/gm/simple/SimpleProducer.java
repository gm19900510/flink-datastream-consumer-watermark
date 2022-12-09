package com.gm.simple;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class SimpleProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleProducer.class);

    private static final int MESSAGE_NUM = 10000;

    private static final String PRODUCER_GROUP = "GID_SIMPLE_PRODUCER";
    private static final String TOPIC = "SOURCE_TOPIC";
    private static final String TAGS = "*";
    private static final String KEY_PREFIX = "KEY";

    private static RPCHook getAclRPCHook() {
        final String accessKey = "${AccessKey}";
        final String secretKey = "${SecretKey}";
        return new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
    }

    private static String transferLongToDate(String dateFormat, Long millSecond) {
        Date time = new Date(millSecond);
        SimpleDateFormat formats = new SimpleDateFormat(dateFormat);
        return formats.format(time);
    }

    public static void main(String[] args) {
        DefaultMQProducer producer =
                new DefaultMQProducer(PRODUCER_GROUP, true, null);
        producer.setNamesrvAddr("120.48.81.173:9876");

        // When using aliyun products, you need to set up channels
        //producer.setAccessChannel(AccessChannel.CLOUD);

        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < MESSAGE_NUM; i++) {
            String content = "Test Message " + i;
            JSONObject json = new JSONObject();
            json.put("content",content);
            long currentDateTimeMillis = System.currentTimeMillis();
            json.put("eventTime",currentDateTimeMillis);
            json.put("time",transferLongToDate("yyyy-MM-dd HH:mm:ss",currentDateTimeMillis));

            Message msg = new Message(TOPIC, TAGS, KEY_PREFIX + i, json.toString().getBytes());
            try {
                SendResult sendResult = producer.send(msg);
                assert sendResult != null;
                System.out.printf(
                        "send result: %s %s %s\n",
                        sendResult.getMsgId(), sendResult.getMessageQueue().toString(),content);
                Random ran = new Random();
                int num = ran.nextInt(3)*1000;
                num=num==0?500:num;
                System.out.println("休眠时间："+num);
                Thread.sleep(num);
            } catch (Exception e) {
                LOGGER.info("send message failed. {}", e.toString());
            }
        }
    }
}