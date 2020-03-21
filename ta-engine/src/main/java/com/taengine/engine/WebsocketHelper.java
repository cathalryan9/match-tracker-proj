package com.taengine.engine;

import java.net.URI;

import javax.websocket.ContainerProvider;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;

public class WebsocketHelper {
	
	Session wordCountSession;
	Session hashtagCountSession;
	Session timeIntervalSession;
	
	public boolean connectWebsockets() {
		
		WebSocketContainer wordCountContainer = null;
		WebSocketContainer timeIntervalContainer = null;
		WebSocketContainer hashtagCountContainer = null;
		try {
		wordCountContainer = ContainerProvider.getWebSocketContainer();
		timeIntervalContainer = ContainerProvider.getWebSocketContainer();
		hashtagCountContainer = ContainerProvider.getWebSocketContainer();
		System.out.println("Connecting");
		wordCountSession = wordCountContainer.connectToServer(Engine.class,
				URI.create("ws://localhost:8080/websocketserver-0.0.1-SNAPSHOT/word_count/spark_1"));
		timeIntervalSession = timeIntervalContainer.connectToServer(Engine.class,
				URI.create("ws://localhost:8080/websocketserver-0.0.1-SNAPSHOT/time_interval_count/spark_1"));
		hashtagCountSession = hashtagCountContainer.connectToServer(Engine.class,
				URI.create("ws://localhost:8080/websocketserver-0.0.1-SNAPSHOT/hashtag_count/spark_1"));
		}
		catch (Exception e) {
			System.out.println("Problem connecting to websocket server");
			// e.printStackTrace();
			// throw e;
			return false;
		}
		return true;
	}

	public void sendToWS(Session session, String message) {
		System.out.println("Message: " + message);
		System.out.println("Message size(bytes): " + message.getBytes().length);
		if (session.isOpen()) {
			System.out.println("Sending to WS" + session.getRequestURI().getPath());
			session.getAsyncRemote().sendText(message);
		}
	}
}
