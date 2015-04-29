package com.chinaxing.lambda.cat.java.erlang.cat;

import com.aliyun.openservices.ons.api.*;
import com.ericsson.otp.erlang.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * Created by LambdaCat on 15/4/29.
 */
public class MessageConsumer {
    private static Logger logger = LoggerFactory.getLogger(MessageConsumer.class);
    private static Consumer consumer;
    private static String topic;
    private static Properties properties;
    private static String appHome;
    private static OtpMbox mbox;
    private static String remoteReceiver;
    private static String remoteNode;
    private static String cookie;

    public static void main(String[] args) {
        loadProperties();
        initConsumer();
        initErlangNode();
        consumerLoop();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                consumer.shutdown();
                mbox.close();
            }
        }));
    }

    private static void loadProperties() {
        appHome = System.getProperty("APP_HOME");
        if (appHome == null || "".equals(appHome)) {
            System.err.println("APP_HOME is null");
            System.exit(-1);
        }
        properties = new Properties();
        try {
            properties.load(new FileInputStream(appHome + "/conf/app.conf"));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void initConsumer() {
        Properties p = new Properties();
        p.setProperty(PropertyKeyConst.AccessKey, properties.getProperty("accessKey"));
        p.setProperty(PropertyKeyConst.SecretKey, properties.getProperty("secretKey"));
        p.setProperty(PropertyKeyConst.ConsumerId, properties.getProperty("consumerId"));

        consumer = ONSFactory.createConsumer(p);
        logger.info("start ONS-Producer");
        try {
            consumer.start();
        } catch (Throwable e) {
            logger.error("start producer failed, die now !", e);
            System.exit(-1);
        }
        logger.info("start done.");
    }

    private static void consumerLoop() {
        topic = properties.getProperty("topic");
        consumer.subscribe(topic, null, new MessageListener() {
            public Action consume(Message message, ConsumeContext context) {
                OtpErlangObject[] msg = new OtpErlangObject[2];
                msg[0] = mbox.self();
                msg[1] = new OtpErlangBinary(message.getBody());
                mbox.send(remoteReceiver, remoteNode, new OtpErlangTuple(msg));
                return Action.CommitMessage;
            }
        });
    }

    private static void initErlangNode() {
        try {
            cookie = properties.getProperty("cookie");
            OtpNode node = new OtpNode("lambda@lambda-cat.local", cookie);
            mbox = node.createMbox("probe_message");
            remoteReceiver = properties.getProperty("remoteReceiver");
            remoteNode = properties.getProperty("remoteNode");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
