package com.zhwb.canal.client;

import com.zhwb.canal.client.receiver.CanalReceiver;
import com.zhwb.canal.client.util.ConfigUtils;

import java.io.IOException;

/**
 * @author zhangwenbin
 * @since 2016/1/19.
 */
public class CanalClientEntry {

    public static void main(String[] args) throws IOException {
        ConfigUtils config = new ConfigUtils("properties/canal.properties");
        CanalReceiver.getInstance(config).listen();
    }
}
