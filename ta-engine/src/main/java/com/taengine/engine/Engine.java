package com.taengine.engine;

import java.io.Serializable;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.util.SerializableBuffer;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import scala.Tuple2;

import javax.websocket.*;

@ClientEndpoint
public class Engine {
	static Session session;
	static final TimeObject timeObj = new TimeObject(); 
	static ArrayList wordsToIgnore = new ArrayList(Arrays.asList("rt","the","and","of","in","vs","at","to","a","we","for","with","an","is"," ",""));
	public static void write(Tuple2<String, Integer> jds, String dbName, String table) {

		String url = DB.getDatabase(dbName);
		Connection conn = null;

		try {
			conn = DriverManager.getConnection(url);
			// Does the word have to be updated or inserted
			String sqlSelectStr = "SELECT * FROM " + table + " WHERE word=" + "'" + jds._1 + "'";
			try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sqlSelectStr)) {
				if (rs.next()) {
					stmt.close();
					// Do an update
					String sqlUpdateStr = "UPDATE " + table + " SET count = count + ?, datetime = ? WHERE word = ?";
					PreparedStatement pstmt = conn.prepareStatement(sqlUpdateStr);
					pstmt.setInt(1, jds._2);
					pstmt.setString(2, LocalDateTime.now().toString());
					pstmt.setString(3, jds._1());
					pstmt.execute();
					pstmt.close();
				} else {
					stmt.close();
					String sql2 = "INSERT INTO " + table + "(word,count,datetime) VALUES(?,?,?) ";
					PreparedStatement pstmt = conn.prepareStatement(sql2);
					pstmt.setString(1, jds._1);
					pstmt.setInt(2, jds._2());
					pstmt.setString(3, LocalDateTime.now().toString());
					pstmt.execute();
					pstmt.close();
				}
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
			conn.close();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}

	}

	public static JavaPairDStream<String, Integer> cleanAndReduce(JavaDStream<String> jds) {
		

		// Parse the text from JSON and split into array of words
		TimeObject to = new TimeObject();
		JavaDStream<String> words = jds.flatMap(f -> {
			JSONObject JO = new JSONObject(f);
			String text = JO.get("text").toString();
			timeObj.timestamp = JO.get("timestamp").toString();
			// replace \u2026 ... sometimes at end of tweets
			return Arrays.asList(text.toLowerCase().replace("'", "").replace(",", " ").split(" ")).iterator();
		}).filter(word -> word.startsWith("-") == false).filter(word -> !Engine.wordsToIgnore.contains(word) );

		// reduce the array to key value pairs
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(word -> {
			Tuple2<String, Integer> wordTuple;

			if (!word.startsWith("#") & word.length() > 1) {
				wordTuple = new Tuple2<>(word.substring(0, 1).toUpperCase() + word.substring(1), 1);
			} else if (!word.startsWith("#")) {
				wordTuple = new Tuple2<>(word.toUpperCase(), 1);
			} else if (word.length() > 2) {
				wordTuple = new Tuple2<>(word.substring(0, 1) + word.substring(1, 2).toUpperCase() + word.substring(2), 1);
			} else {
				wordTuple = new Tuple2<>(word.substring(0, 1) + word.substring(1, 2).toUpperCase(), 1);
			}
			return wordTuple;
		}).reduceByKey((i1, i2) -> i1 + i2);
		
		wordCounts = wordCounts.mapToPair(pair -> {JSONObject JO = new JSONObject();
									System.out.println("finalize the json");
									JO.put("text", pair._1);
									JO.put("count", pair._2);
									JO.put("timestamp", timeObj.timestamp);
									String json = JO.toString();
									Tuple2<String, Integer> pairWithJson = new Tuple2<>(json, pair._2);
									
									return pairWithJson;});

		return wordCounts;

	}

	public static void sendToWS(Tuple2<String, Integer> message) {
		// TODO: send in json format with the count field
		session.getAsyncRemote().sendText(message._1);
		System.out.println("Sending to WS");
		System.out.println(message._1);
	}

	public static void main(String[] args) throws Exception {
		System.out.println(args[0]);
		wordsToIgnore.add(args[0]);
		//DB.createNewDatabase(DB.getDatabase(System.getProperty("user.dir") + "\\src\\main\\resources\\db_1.db"));
		JavaSparkContext sc = new JavaSparkContext("local[2]", "TwitterStream");
		sc.setLogLevel("WARN");
		JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(2000));
		// jssc.checkpoint("file:///tmp/spark");
		// Add a custom receiver for RabbitMQ.
		// https://spark.apache.org/docs/2.3.1/streaming-custom-receivers.html
		JavaDStream<String> customReceiverStream = jssc.receiverStream(new CustomReceiver());
		WebSocketContainer container = null;

		try {
			container = ContainerProvider.getWebSocketContainer();
			System.out.println("Connecting");
			session = container.connectToServer(Engine.class,
					URI.create("ws://localhost:8080/websocketserver-0.0.1-SNAPSHOT/data/spark_1"));
		} catch (Exception e) {
			System.out.println("Problem connecting to websocket server");
			// e.printStackTrace();
			// throw e;
		}

		JavaPairDStream<String, Integer> wordCounts = cleanAndReduce(customReceiverStream);
		wordCounts.foreachRDD(s -> {
			s.foreach(f -> {// write(f, System.getProperty("user.dir")+"\\src\\main\\resources\\db_1.db",
							// "words");
							
							 sendToWS(f);
							
			});
		});

		System.out.println("BEFORE start");
		jssc.start();

		jssc.awaitTermination();

	}

}

// Could not save the timestamp value without a class. Hacky :(
class TimeObject implements Serializable {
	private static final long serialVersionUID = 1L;
	String timestamp;
}