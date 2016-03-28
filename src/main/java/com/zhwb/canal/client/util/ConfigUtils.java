package com.zhwb.canal.client.util;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

/**
 * 读取classpath下面的properties文件
 * @author zhangwenbin
 */
public class ConfigUtils implements Serializable {

    private static final long serialVersionUID = -3761465817152624262L;

    private Map<String, String> props = Maps.newHashMap();

    public ConfigUtils(String fileName) throws IOException {
        props = getMap(fileName);
    }

    public String getValue(String key) {
        if (props.containsKey(key)) {
            return props.get(key);
        } else
            return null;
    }

    public String getString(String key) {
        if (props.containsKey(key)) {
            return props.get(key);
        } else
            return null;
    }

    public int getInt(String key) {
        if (props.containsKey(key)) {
            String value = props.get(key);
            return Integer.parseInt(value);
        } else
            return -1;
    }

    public boolean getBoolean(String key) {
        boolean isOK = false;
        if (props.containsKey(key)) {
            String value = props.get(key);
            if (value.equalsIgnoreCase("Y")||value.equalsIgnoreCase("true")) {
                isOK = true;
            }
        }
        return isOK;
    }

    public Map<String, String> getMap(String propertyName) throws IOException {
        if (props.isEmpty()) {
            Properties properties = new Properties();
            properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(propertyName));
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                props.put(entry.getKey().toString(), entry.getValue().toString());
            }
            return props;
        }
        return props;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("props", props)
                .toString();
    }
}
