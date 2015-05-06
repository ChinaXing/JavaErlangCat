package com.chinaxing.lambda.cat.java.erlang.cat;

import com.aliyun.openservices.ons.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by LambdaCat on 15/5/4.
 */
public class ONSMessageConsumer implements MessageConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ONSMessageConsumer.class);
    String topic;
    Consumer consumer;

    public ONSMessageConsumer(Properties properties) {
        Properties p = new Properties();
        p.setProperty(PropertyKeyConst.AccessKey, properties.getProperty("accessKey"));
        p.setProperty(PropertyKeyConst.SecretKey, properties.getProperty("secretKey"));
        p.setProperty(PropertyKeyConst.ConsumerId, properties.getProperty("consumerId"));

        consumer = ONSFactory.createConsumer(p);
        logger.info("start ONS-Producer");
        try {
            consumer.start();
            topic = properties.getProperty("topic");
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                public void run() {
                    consumer.shutdown();
                }
            }));
        } catch (Throwable e) {
            logger.error("start producer failed, die now !", e);
            System.exit(-1);
        }
        logger.info("start done.");
    }

    public void addListener(final MessageListener listener) {
        consumer.subscribe(topic, null, new com.aliyun.openservices.ons.api.MessageListener() {
            public Action consume(Message message, ConsumeContext context) {
                listener.OnMessage(message.getBody());
                return Action.CommitMessage;
            }
        });
    }
}
