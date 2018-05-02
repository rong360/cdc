package com.rong360.rabbitmq;

import com.rabbitmq.client.*;
import com.rong360.binlogutil.GlobalConfig;
import com.rong360.main.CdcClient;
import com.rong360.model.QueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * @author zhangtao@rong360.com
 */
public class RabbitMessageListener implements CdcClient.MessageListener {
    private final static Logger logger = LoggerFactory.getLogger(RabbitMessageListener.class);
    private volatile static Connection connection = null;
    private static ConcurrentHashMap<String, Object> channelMap = new ConcurrentHashMap<>();

    private static synchronized Connection getConnection() {
        if (connection == null) {
            logger.info("connection is null,init it!");
            ConnectionFactory factory = new ConnectionFactory();
            String[] tmp = GlobalConfig.rabbitmq_host.split(",");
            Address[] addresses = new Address[tmp.length];
            for (int i = 0; i < tmp.length; i++) {
                addresses[i] = new Address(tmp[i], GlobalConfig.rabbitmq_port);
            }
            factory.setVirtualHost(GlobalConfig.rabbitmq_vhost);
            factory.setUsername(GlobalConfig.rabbitmq_username);
            factory.setPassword(GlobalConfig.rabbitmq_password);
            factory.setNetworkRecoveryInterval(100);

            factory.setTopologyRecoveryEnabled(false);
            factory.setAutomaticRecoveryEnabled(true);
            factory.setConnectionTimeout(5 * 1000);
            factory.setRequestedHeartbeat(20);
            try {
                connection = factory.newConnection(addresses);
            } catch (IOException | TimeoutException e) {
                connection = null;
                logger.warn("create rabbitmq new connection", e);
            }
        }
        return connection;
    }

    public static Channel getChannel() throws Exception {
        String threadName = Thread.currentThread().getName();
        Channel channel;
        channel = (Channel) channelMap.get(threadName);
        if (channel == null) {
            logger.info("thread:{},channel is null ,create one", threadName);
            channel = getConnection().createChannel();
            channelMap.put(threadName, channel);
        }
        return channel;
    }

    public boolean publishBatch(List<QueueData> dataList) {
        boolean isSuc = true;
        Channel channel;
        try {
            channel = getChannel();
            for (QueueData qd : dataList) {
                channel.basicPublish(GlobalConfig.rabbitmq_exchangename, qd.getRoutingKey(),
                        new AMQP.BasicProperties().builder().contentType("text/plain").deliveryMode(2).priority(1).build(),
                        qd.getMessage().getBytes("UTF-8"));
            }
        } catch (Exception e) {
            logger.warn("publishBatch IOException:", e);
            isSuc = false;
        }
        return isSuc;
    }

}
