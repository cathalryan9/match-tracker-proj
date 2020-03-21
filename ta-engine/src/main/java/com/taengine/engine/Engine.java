package com.taengine.engine;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

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
import org.json.JSONObject;

import scala.Tuple2;

import javax.websocket.*;

@ClientEndpoint
public class Engine {
	static WebsocketHelper wh = new WebsocketHelper(); 

	static DataMapHelper dmh = new DataMapHelper();

	static ArrayList<String> wordsToIgnore = new ArrayList<String>(
			Arrays.asList("rt", "the", "and", "&amp", "i", "you", "from", "are", "was", "what", "has", "this", "of",
					"if", "it", "in", "vs", "at", "to", "a", "we", "for", "with", "an", "is", " ", ""));


	public static JavaRDD<String> cleanAndReduce(JavaRDD<String> jds) {
		// Transformations are lazy. Need to call an action to trigger them e.g count()
		System.out.println("clean and reduce");
		// Parse the text from JSON and split into array of words
		jds = jds.flatMap(f -> {
			JSONObject JO = new JSONObject(f);

			String text = JO.getString("text");
			System.out.println(text);
			dmh.addToInterval(f);

			// replace \u2026 ... sometimes at end of tweets
			return Arrays.asList(text.toLowerCase().replace("'", "").replace(",", " ").split(" ")).iterator();
		}).filter(word -> word.startsWith("-") == false).filter(word -> !Engine.wordsToIgnore.contains(word));

		// Split into 2 streams
		JavaRDD<String> hashtags = jds.filter(f -> f.startsWith("#"));
		jds = jds.filter(f -> !f.startsWith("#"));

		JavaPairRDD<String, Integer> hashtagCounts = hashtags.mapToPair(hashtag -> {
			Tuple2<String, Integer> hashtagTuple = new Tuple2<>(
					hashtag.substring(0, 1) + hashtag.substring(1, 2).toUpperCase() + hashtag.substring(2), 1);
			return hashtagTuple;
		});

		// reduce the array to key value pairs
		JavaPairRDD<String, Integer> wordCounts = jds.mapToPair(word -> {
			Tuple2<String, Integer> wordTuple = null;

			if (word.length() > 1) {
				wordTuple = new Tuple2<>(word.substring(0, 1).toUpperCase() + word.substring(1), 1);
			} else {
				wordTuple = new Tuple2<>(word.substring(0, 1).toUpperCase(), 1);
			}
			return wordTuple;
		}).reduceByKey((i1, i2) -> i1 + i2);

		JavaRDD<String> jsonObj = wordCounts.map(pair -> {
			JSONObject JO = new JSONObject();
			JO.put("text", pair._1);
			JO.put("count", pair._2);
			// JO.put("timestamp", timeObj.timestamp);
			String json = JO.toString();
			if (!dmh.wordCountMap.containsKey(pair._1)) {
				dmh.wordCountMap.put(pair._1, pair._2);
				dmh.currentIntervalMap.put(pair._1, pair._2);
			} else if (!dmh.currentIntervalMap.containsKey(pair._1)) {
				System.out.println("not in current interval");
				dmh.wordCountMap.replace(pair._1, dmh.wordCountMap.get(pair._1) + pair._2);
				dmh.currentIntervalMap.put(pair._1, pair._2);
			} else {
				dmh.wordCountMap.replace(pair._1, dmh.wordCountMap.get(pair._1) + pair._2);
				dmh.currentIntervalMap.put(pair._1, dmh.currentIntervalMap.get(pair._1) + pair._2);
			}
			return json;
		});
		hashtagCounts.foreach(pair -> {
			if (!dmh.hashtagCountMap.containsKey(pair._1)) {
				dmh.hashtagCountMap.put(pair._1, pair._2);
			} else {
				dmh.hashtagCountMap.replace(pair._1, dmh.hashtagCountMap.get(pair._1) + pair._2);
			}

		});

		System.out.println("Word count: " + jsonObj.count());
		System.out.println("Hashtag count: " + hashtagCounts.count());

		return jsonObj;

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
		
		wh.connectWebsockets();

		JavaDStream<String> cleanedStream = customReceiverStream.transform(rdd -> {
			return cleanAndReduce(rdd);
		});

		cleanedStream.foreachRDD(s -> {

			List<JSONObject> wordCountList = DataMapHelper.createSortedList(dmh.wordCountMap);
			if (wordCountList != null) {
				wh.sendToWS(wh.wordCountSession, wordCountList.toString());
			}

			List<JSONObject> hashtagCountList = DataMapHelper.createSortedList(dmh.hashtagCountMap);
			if (hashtagCountList != null) {
				wh.sendToWS(wh.hashtagCountSession, hashtagCountList.toString());
			}

			// ws.sendToWS(timeIntervalSession, messageArrayAsString );
		});

		jssc.start();

		jssc.awaitTermination();

	}

}

