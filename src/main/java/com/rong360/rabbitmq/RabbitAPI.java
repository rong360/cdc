package com.rong360.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rong360.binlogutil.GlobalConfig;
import com.rong360.main.CdcClient;
import com.rong360.model.QueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

/**
 * 
 * @author zhangtao@rong360.com
 *
 */
public class RabbitAPI implements CdcClient.MessageListener{
	private final static Logger logger = LoggerFactory.getLogger(RabbitAPI.class);
	private volatile static Connection connection = null;
	private volatile static Channel channel = null;

	private static Connection getConnection(){
		if(connection == null){
			logger.info("connection is null,init it!");
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(GlobalConfig.rabbitmq_host);
			factory.setPort(GlobalConfig.rabbitmq_port);
			factory.setVirtualHost(GlobalConfig.rabbitmq_vhost);
			factory.setUsername(GlobalConfig.rabbitmq_username);
			factory.setPassword(GlobalConfig.rabbitmq_password);
			factory.setNetworkRecoveryInterval(5000);
			//指定连接池
			ExecutorService service = Executors.newFixedThreadPool(5);
			factory.setSharedExecutor(service);
			factory.setTopologyRecoveryEnabled(false);
			factory.setAutomaticRecoveryEnabled(true);
			factory.setConnectionTimeout(5*1000);
			factory.setRequestedHeartbeat(5);
			try {
				connection = factory.newConnection();
			} catch (IOException | TimeoutException e) {
				logger.warn(e.getMessage());
			}
		}
		return connection;
	}

	public static Channel getChannel(){
		if (channel == null){
			try {
				synchronized (RabbitAPI.class) {
					if (channel == null) {
						channel = getConnection().createChannel();
					}
				}
			} catch (Exception e) {
				logger.warn("get channel Exception:{}", e.getMessage());
			}
		}
		return channel;
	}

	public boolean publishBatch(List<QueueData> dataList){
		boolean isSuc = true;
		Channel channel;
		try {
			channel = getChannel();
			for(QueueData qd:dataList){
				channel.basicPublish(GlobalConfig.rabbitmq_exchangename, qd.getRoutingKey(),
						new AMQP.BasicProperties().builder().contentType("text/plain").deliveryMode(2).priority(1).build(),
						qd.getMessage().getBytes("UTF-8"));
			}
		} catch (Exception e) {
			logger.warn("publishBatch IOException:" + e.getMessage());
			isSuc = false;
			try {
				RabbitAPI.channel.close();
				RabbitAPI.connection.close();
			} catch (Exception ec) {
				ec.printStackTrace();
			}
			RabbitAPI.connection = null;
			RabbitAPI.channel = null;
		}
		return isSuc;
	}

}
