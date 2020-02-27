package com.taengine.engine;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.util.concurrent.TimeoutException;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;


import com.rabbitmq.client.DeliverCallback;
import com.rabbitmqwrapper.MQwrapper.RabbitMQWrapper;

import org.json.*;

public class CustomReceiver extends Receiver<String> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String host = null;
	private int port = -1;
	private Receiver<String> inst;

	public CustomReceiver() {
		super(StorageLevel.MEMORY_AND_DISK_2());
		inst = this;
	}
	
	public void setHost(String host) {
		this.host = host; 
	}
	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public void onStart() {
		new Thread(this::receive).start();
	}

	@Override
	public void onStop() {
		// TODO Auto-generated method stub

	}

	
	private void receive(){
		// open new socket to stream the data
		DeliverCallback deliverCallback = (consumerTag, delivery) -> {
		      String messageStr = new String(delivery.getBody(), "UTF-8");
		      JSONObject tweetJSONObj = new JSONObject(messageStr);
		      
		      String text = null;
				try {
					JSONObject extendedTweetJSONObj = new JSONObject(tweetJSONObj.get("extended_tweet"));
					text = extendedTweetJSONObj.getString("full_text");
				} catch(JSONException e) {
					text = tweetJSONObj.getString("text");
				}
		      
		      String timestamp = tweetJSONObj.get("timestamp_ms").toString();
		      tweetJSONObj = new JSONObject();
		      tweetJSONObj.put("text", text);
		      tweetJSONObj.put("timestamp", timestamp);
		      System.out.println(" [x] Received '" + tweetJSONObj.toString() + "'");
		      store(tweetJSONObj.toString());
		  };
		RabbitMQWrapper rmq = RabbitMQWrapper.getrmqw("IncomingTweetQueue");
	    try {
	      // this is where our message reading is done and put into spark

	      rmq.getChannel().basicConsume( rmq.getQueueName(), true, deliverCallback, consumerTag -> { });
	      System.out.println("after consume");
	      // Until stopped or connection broken continue reading
	      while (!isStopped() && (rmq.getChannel().isOpen())) {
	      }      

	    } catch(ConnectException ce) {
	        // restart if could not connect to server
	        restart("Could not connect", ce);
	    }
	    catch(Throwable e) {
	    	restart("Error receiving data", e);
	    }
	    finally {
	    		try {
					rmq.getChannel().close();
				} catch (IOException | TimeoutException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

	    }
	}

}
