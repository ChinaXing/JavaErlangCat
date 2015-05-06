package com.chinaxing.lambda.cat.java.erlang.cat;

import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

/**
 * Created by LambdaCat on 15/5/4.
 */
public class MockMessageConsumer implements MessageConsumer {
    Random random = new Random(System.currentTimeMillis());
    static Gson gson = new Gson();

    public void addListener(final MessageListener listener) {
        new Thread(new Runnable() {
            public void run() {
                try {

                    while (true) {
                        Map<String, Object> map = new HashMap<String, Object>();
                        {
                            map.put("sn", "P_00000000000000_" + Integer.valueOf(random.nextInt(4)));
                            map.put("devMac", "00:16:3E:02:2B:3D");
                            map.put("rssi", -1 * random.nextInt(80));
                            map.put("arriveLeave", random.nextBoolean() ? 1 : 0);
                        }
                        listener.OnMessage(gson.toJson(map).getBytes());
                        Thread.sleep(1000);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(0);
                }
            }
        }).start();
    }
}
