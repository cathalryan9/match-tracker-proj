import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import javax.websocket.*;
import javax.websocket.server.*;
import javax.websocket.server.ServerEndpoint;

import websocketserver.*;


@ServerEndpoint(value = "/data/{username}",
decoders = MessageDecoder.class, 
encoders = MessageEncoder.class)
public class WebSocketServer {
	
    private Session session;
    private static Set<WebSocketServer> dataEndpoints 
      = new HashSet<WebSocketServer>();
    private static HashMap<String, String> users = new HashMap<String, String>();
 
    
	@OnOpen
    public void onOpen(Session session, 
	      @PathParam("username") String username) throws IOException {
	  
	        this.session = session;
	        dataEndpoints.add(this);
	        users.put(session.getId(), username);
	 
	        Message message = new Message();
	        message.setFrom(username);
	        message.setContent("Connected!");
	        //read from database and send data
	        try {
				broadcast(message);
			} catch (EncodeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    }
 
    @OnMessage
    public void onMessage(Session session, Message message) throws IOException {
        System.out.println(message);
    }
 
    @OnClose
    public void onClose(Session session) throws IOException {
    	dataEndpoints.remove(this);
        Message message = new Message();
        message.setFrom(users.get(session.getId()));
        message.setContent("Disconnected!");
        //broadcast(message);
    }
 
    @OnError
    public void onError(Session session, Throwable throwable) {
        // Do error handling here
    }
    private static void broadcast(Message message) 
    	      throws IOException, EncodeException {
    	  
    	        dataEndpoints.forEach(endpoint -> {
    	            synchronized (endpoint) {
    	                try {
    	                    endpoint.session.getBasicRemote().
    	                      sendObject(message);
    	                } catch (IOException | EncodeException e) {
    	                    e.printStackTrace();
    	                }
    	            }
    	        });
    	    }


}
