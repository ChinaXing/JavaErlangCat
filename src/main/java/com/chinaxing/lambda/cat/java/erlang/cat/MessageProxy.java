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
public class MessageProxy {
    private static Logger logger = LoggerFactory.getLogger(MessageProxy.class);
    private static MessageConsumer consumer;
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
        consumer = new ONSMessageConsumer(properties);
//        consumer = new MockMessageConsumer();
    }

    private static void consumerLoop() {
        consumer.addListener(new MessageListener() {
            public void OnMessage(byte[] message) {
                OtpErlangObject[] msg = new OtpErlangObject[2];
                msg[0] = mbox.self();
                msg[1] = new OtpErlangBinary(message);
                mbox.send(remoteReceiver, remoteNode, new OtpErlangTuple(msg));
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
