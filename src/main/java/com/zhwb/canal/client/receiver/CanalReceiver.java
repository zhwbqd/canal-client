package com.zhwb.canal.client.receiver;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.zhwb.canal.client.exporter.CanalExporter;
import com.zhwb.canal.client.util.ConfigUtils;
import com.zhwb.canal.client.domain.CanalEvent;
import com.zhwb.canal.client.parser.CanalParser;
import com.zhwb.canal.client.util.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author zhangwenbin
 * @since 2016/1/19.
 */
public final class CanalReceiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(CanalReceiver.class);
    private static CanalReceiver RECEIVER;

    private CanalConnector connector;
    private int batchSize;
    private long batchInterval;
    private ConfigUtils config;

    private CanalReceiver(ConfigUtils config) {
        this.config = config;
        this.batchSize = config.getInt("canal.receiver.batch.size");
        this.batchInterval = config.getInt("canal.receiver.batch.interval");

        String connectType = config.getString("canal.connect.type");
        String connectStr = config.getString("canal.connect.str");
        String connectDestination = config.getString("canal.connect.desc");
        String subscribe = config.getString("canal.receiver.subscribe");

        if (Objects.equals(connectType, ConfigConstant.CONNECT_TYPE_SIMPLE)) {
            connector = CanalConnectors.newSingleConnector(new InetSocketAddress(connectStr, config.getInt("canal.connect.port")),
                    connectDestination, "", "");
        } else if (Objects.equals(connectType, ConfigConstant.CONNECT_TYPE_ZK)) {
            connector = CanalConnectors.newClusterConnector(connectStr, connectDestination, "", "");
        } else {
            throw new UnsupportedOperationException("connect type is invalid, type: " + connectType);
        }

        //连接, 并初始化
        connector.connect();
        connector.subscribe(subscribe);
        connector.rollback();

        LOGGER.info("Receiver init success, ready to receive, connect str:{}, subscribe:{}, connect dest:{}, batch size:{}, batch interval:{}",
                new Object[]{connectStr, subscribe, connectDestination, batchSize, batchInterval});

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                connector.disconnect();
                LOGGER.info("Receiver shutdown, release the connection");
            }
        });
    }

    public static synchronized CanalReceiver getInstance(ConfigUtils config) {
        if (RECEIVER == null) {
            RECEIVER = new CanalReceiver(config);
        }
        return RECEIVER;
    }

    public void listen() {
        long batchId = -1;
        while (true) {
            try {
                Message message = connector.getWithoutAck(batchSize, batchInterval, TimeUnit.SECONDS);
                batchId = message.getId();
                List<CanalEntry.Entry> entries = message.getEntries();
                List<CanalEvent> canalEventList = CanalParser.parse(entries);
                if (CollectionUtils.isEmpty(canalEventList)) {
                    continue;
                }
                CanalExporter.getExporter(config).export(canalEventList);
                LOGGER.info("latest batchId: {}", batchId);
            } catch (CanalClientException e) {
                LOGGER.error("error while listen canal server, batch: {}, e: {}", batchId, e.getMessage());
            } finally {
                connector.ack(batchId);
            }
        }
    }
}
