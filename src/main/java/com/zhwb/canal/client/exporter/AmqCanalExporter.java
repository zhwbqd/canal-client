package com.zhwb.canal.client.exporter;

import com.alibaba.fastjson.JSON;
import com.zhwb.canal.client.domain.CanalEvent;
import com.zhwb.canal.client.util.ConfigUtils;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.lang.IllegalStateException;
import java.util.List;

/**
 * @author zhangwenbin
 * @since 2016/1/19.
 */
public class AmqCanalExporter extends CanalExporter {

    private static AmqCanalExporter AMQ_CANAL_EXPORTER;
    private static final Logger LOGGER = LoggerFactory.getLogger(AmqCanalExporter.class);
    private Connection connection;
    private String queueName;

    private AmqCanalExporter(ConfigUtils config) {
        String connectStr = config.getString("canal.export.str");
        String connectUser = config.getString("canal.export.user");
        String connectPwd = config.getString("canal.export.pwd");
        queueName = config.getString("canal.export.queue");
        try {
            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(connectUser, connectPwd, connectStr);
            connection = connectionFactory.createConnection();
            connection.start();

            LOGGER.info("AMQ init success, connectstr:{}, queueName:{}", connectStr, queueName);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        connection.close();
                    } catch (JMSException ignored) {
                    }
                    LOGGER.info("AMQ shutdown, release the connection");
                }
            });
        } catch (JMSException e) {
            LOGGER.error("AMQ init failed", e);
            throw new IllegalStateException("AMQ init failed", e);
        }
    }

    @Override
    public void exportReal(List<CanalEvent> canalEventList) {
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(destination);
            for (CanalEvent event : canalEventList) {
                TextMessage message = session.createTextMessage(JSON.toJSONString(event));
                producer.send(message);
            }
        } catch (JMSException e) {
            LOGGER.error("send to AMQ failed, e={}, data={}", e.getMessage());
        }
    }

    public static AmqCanalExporter getInstance(ConfigUtils config) {
        if (AMQ_CANAL_EXPORTER == null) {
            AMQ_CANAL_EXPORTER = new AmqCanalExporter(config);
        }
        return AMQ_CANAL_EXPORTER;
    }
}
