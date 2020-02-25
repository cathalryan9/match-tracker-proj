import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import javax.websocket.*;
import javax.websocket.server.*;
import javax.websocket.server.ServerEndpoint;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import websocketserver.*;

@ServerEndpoint(value = "/word_count/{username}", decoders = MessageDecoder.class, encoders = MessageEncoder.class)
public class WordCountWebSocket {

	private Session session;
	private static Set<WordCountWebSocket> clientSessions = new HashSet<WordCountWebSocket>();
	private static Set<WordCountWebSocket> sparkSessions = new HashSet<WordCountWebSocket>();

	@OnOpen
	public void onOpen(Session session, @PathParam("username") String username) throws IOException {
		session.setMaxTextMessageBufferSize(256000);
		System.out.println("open");
		this.session = session;
		if (username.equals("spark_1")) {
			sparkSessions.add(this);
			System.out.println("Spark Server Connected");
		} else {
			clientSessions.add(this);
			System.out.println("Client Connected!");
		}
		//broadcast(message);
	}

	@OnMessage
	public void onMessage(Session session, Message message) {
		Message newMessage = new Message();
		newMessage.setFrom("spark");
		//System.out.println(message.getContent());
		newMessage.setContent(message.getContent());
		broadcast(newMessage);
	}

	@OnClose
	public void onClose(Session session) throws IOException {

		if (clientSessions.remove(this)) {
			sparkSessions.remove(this);
		}
		//broadcast(message);
	}

	@OnError
	public void onError(Session session, Throwable throwable) {
		// Do error handling here
	}

	// Send the dataset to the clients
	private static void broadcast(Message message) {
		System.out.println("new message");
		clientSessions.forEach(endpoint -> {
			synchronized (endpoint) {
				try {
					endpoint.session.getBasicRemote().sendObject(message);
				} catch (IOException | EncodeException e) {
					e.printStackTrace();
				}
				System.out.println("end of old message");
			}
		});
	}

}