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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.util.SerializableBuffer;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import scala.Tuple2;

import javax.websocket.*;

@ClientEndpoint
public class Engine {
	static Session wordCountSession;
	static Session hashtagCountSession;
	static Session timeIntervalSession;
	static final TimeObject timeObj = new TimeObject();
	static ArrayList<String> wordsToIgnore = new ArrayList<String>(
			Arrays.asList("rt", "the", "and", "&amp", "i", "you", "from", "are", "was", "what", "has", "this", "of",
					"if", "it", "in", "vs", "at", "to", "a", "we", "for", "with", "an", "is", " ", ""));
	// current interval
	static Date currentTimeObj = new Date();
	// mapofcurrentinterval
	static Map<String, Integer> currentIntervalMap = new HashMap<String, Integer>();
	// map of all intervals
	static Map<Long, String> intervalsMap = new HashMap<Long, String>();
	// map of all words
	static Map<String, Integer> wordCountMap = new HashMap<String, Integer>();
	// map of hashtags
	static Map<String, Integer> hashtagCountMap = new HashMap<String, Integer>();

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

	public static JavaRDD<String> cleanAndReduce(JavaRDD<String> jds) {
		// Transformations are lazy. Need to call an action to trigger them e.g count()
		System.out.println("clean and reduce");
		// Parse the text from JSON and split into array of words
		jds = jds.flatMap(f -> {
			// System.out.println("flatmap");
			JSONObject JO = new JSONObject(f);

			String text = JO.getString("text");
			System.out.println(text);
			if (currentTimeObj == null) {
				currentTimeObj = new Date(Long.parseLong(JO.get("timestamp").toString()));
			} else if (Long.parseLong(JO.get("timestamp").toString()) > (long) (currentTimeObj.getTime() + 60000)) {

				System.out.println("end of interval");
				// error on line 166 v
				String word = Collections.max(currentIntervalMap.entrySet(), Map.Entry.comparingByValue()).getKey();
				intervalsMap.put(Long.parseLong(JO.get("timestamp").toString()), word);
				currentIntervalMap.clear();
				currentTimeObj.setTime(currentTimeObj.getTime() + 60000);
			}

			// replace \u2026 ... sometimes at end of tweets
			return Arrays.asList(text.toLowerCase().replace("'", "").replace(",", " ").split(" ")).iterator();
		}).filter(word -> word.startsWith("-") == false).filter(word -> !Engine.wordsToIgnore.contains(word));

		JavaRDD<String> hashtags = jds.filter(f -> f.startsWith("#"));

		JavaPairRDD<String, Integer> hashtagCounts = hashtags.mapToPair(hashtag -> {
			Tuple2<String, Integer> hashtagTuple = new Tuple2<>(
					hashtag.substring(0, 1) + hashtag.substring(1, 2).toUpperCase() + hashtag.substring(2), 1);
			return hashtagTuple;
		});

		// reduce the array to key value pairs
		JavaPairRDD<String, Integer> wordCounts = jds.mapToPair(word -> {
			Tuple2<String, Integer> wordTuple;
			if (!word.startsWith("#") & word.length() > 1) {
				wordTuple = new Tuple2<>(word.substring(0, 1).toUpperCase() + word.substring(1), 1);
			} else if (!word.startsWith("#")) {
				wordTuple = new Tuple2<>(word.toUpperCase(), 1);
			} else if (word.length() > 2) {
				wordTuple = new Tuple2<>(word.substring(0, 1) + word.substring(1, 2).toUpperCase() + word.substring(2),
						1);
			} else {
				wordTuple = new Tuple2<>(word.substring(0, 1) + word.substring(1, 2).toUpperCase(), 1);
			}
			return wordTuple;
		}).reduceByKey((i1, i2) -> i1 + i2);

		JavaRDD<String> jsonObj = wordCounts.map(pair -> {
			JSONObject JO = new JSONObject();
			JO.put("text", pair._1);
			JO.put("count", pair._2);
			//JO.put("timestamp", timeObj.timestamp);
			String json = JO.toString();
			if (!wordCountMap.containsKey(pair._1)) {
				wordCountMap.put(pair._1, pair._2);
				currentIntervalMap.put(pair._1, pair._2);
			} else if (!currentIntervalMap.containsKey(pair._1)) {
				System.out.println("not in current interval");
				wordCountMap.replace(pair._1, wordCountMap.get(pair._1) + pair._2);
				currentIntervalMap.put(pair._1, pair._2);
			} else {
				wordCountMap.replace(pair._1, wordCountMap.get(pair._1) + pair._2);
				currentIntervalMap.put(pair._1, currentIntervalMap.get(pair._1) + pair._2);
			}
			return json;
		});
		hashtagCounts.foreach(pair -> {
			if (!hashtagCountMap.containsKey(pair._1)) {
				hashtagCountMap.put(pair._1, pair._2);
			} else {
				hashtagCountMap.replace(pair._1, hashtagCountMap.get(pair._1) + pair._2);
			}

		});

		System.out.println("Word count: " + jsonObj.count());
		System.out.println("Hashtag count: " + hashtagCounts.count());
		System.out.println("end of clean and reduce");

		return jsonObj;

	}

	public static void sendToWS(Session session, String message) {
		System.out.println("Message: " + message);
		System.out.println("Message size(bytes): " + message.getBytes().length);
		if (session.isOpen()) {
			System.out.println("Sending to WS" + session.getRequestURI().getPath());
			session.getAsyncRemote().sendText(message);
		}
	}

	public static List<JSONObject> createSortedList(Map<String, Integer> m) {
		if (!m.isEmpty()) {
			List<JSONObject> messageList = new ArrayList<JSONObject>();
			for (Map.Entry<String, Integer> entry : m.entrySet()) {
				// construct json object
				JSONObject JO = new JSONObject();
				JO.put("text", entry.getKey());
				JO.put("count", entry.getValue());
				String json = JO.toString();
				messageList.add(JO);
			}
			messageList.sort((o1, o2) -> {
				if ((int) o1.get("count") < (int) o2.get("count")) {
					return 1;
				} else if ((int) o1.get("count") > (int) o2.get("count")) {
					return -1;
				}
				return 0;
			});
			// If the number of words is > 30, take the largest 30 by count
			if (messageList.size() > 30) {
				messageList = messageList.subList(0, 30);
			}
			return messageList;
		}
		return null;
	}

	public static void main(String[] args) throws Exception {
		System.out.println(args[0]);
		wordsToIgnore.add(args[0].toLowerCase());
		// DB.createNewDatabase(DB.getDatabase(System.getProperty("user.dir") +
		// "\\src\\main\\resources\\db_1.db"));
		JavaSparkContext sc = new JavaSparkContext("local[2]", "TwitterStream");
		sc.setLogLevel("WARN");
		JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(4000));
		// jssc.checkpoint("file:///tmp/spark");
		// Add a custom receiver for RabbitMQ.
		// https://spark.apache.org/docs/2.3.1/streaming-custom-receivers.html
		JavaReceiverInputDStream<String> customReceiverStream = jssc.receiverStream(new CustomReceiver());
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
		} catch (Exception e) {
			System.out.println("Problem connecting to websocket server");
			// e.printStackTrace();
			// throw e;
		}

		JavaDStream<String> cleanedStream = customReceiverStream.transform(rdd -> {
			return cleanAndReduce(rdd);
		});

		cleanedStream.foreachRDD(s -> {

			List<JSONObject> wordCountList = createSortedList(wordCountMap);
			if (wordCountList != null) {
				sendToWS(wordCountSession, wordCountList.toString());
			}
			
			List<JSONObject> hashtagCountList = createSortedList(hashtagCountMap);
			if (hashtagCountList != null) {
				sendToWS(hashtagCountSession, hashtagCountList.toString());
			}

			// sendToWS(timeIntervalSession, messageArrayAsString );
		});

		jssc.start();

		jssc.awaitTermination();

	}

}

// Could not save the timestamp value without a class. Hacky :(
class TimeObject implements Serializable {
	private static final long serialVersionUID = 1L;
	String timestamp;
}