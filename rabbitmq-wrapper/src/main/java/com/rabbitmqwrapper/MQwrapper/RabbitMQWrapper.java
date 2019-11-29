package com.rabbitmqwrapper.MQwrapper;	
// Class design to make just one connection with MQ
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class RabbitMQWrapper{

	private String QUEUE_NAME;
	private Channel channel;
	private Connection connection;
	private static RabbitMQWrapper rmqwSingleton;
	private RabbitMQWrapper(String queueName){
		// Open connection to queue
			
				QUEUE_NAME = queueName;
				ConnectionFactory factory = new ConnectionFactory();
				factory.setHost("localhost"); 	
				try 
				{
					connection = factory.newConnection();
					channel = connection.createChannel();
					channel.queueDeclare(QUEUE_NAME, false, false, false, null);
					rmqwSingleton = this;
				}
				catch(Exception e){
					System.out.println(e);
					System.exit(-1);
				}

	}
	public static RabbitMQWrapper getrmqw(String queueName) {
		if(rmqwSingleton == null) {
			return new RabbitMQWrapper(queueName);
		}
		else {
			return rmqwSingleton;
		}
	}
	public void writeToQueue(String message)throws Exception{
		// Write to queue
		this.channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));
        System.out.println(" [x] Sent '" + message + "'");
	}
	
	DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
        };
	
	public String readFromQueue() throws Exception{
		channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
		return "message";
	}
	public Channel getChannel() {
		return channel;
	
	}
	public String getQueueName() {
		return this.QUEUE_NAME;
	}


}