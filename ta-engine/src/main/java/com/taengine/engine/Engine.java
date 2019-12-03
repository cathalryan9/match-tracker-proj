package com.taengine.engine;

import java.util.Arrays;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

public class Engine {
	public static void write(JavaPairRDD<String, Integer> jds) {
		//TODO: Write to database
		System.out.println("Write method");
		jds.foreachAsync(f -> System.out.println(f));
	}

	public static JavaPairDStream<String, Integer> cleanAndReduce(JavaDStream<String> jds) {
		JavaDStream<String> words = jds
				.flatMap(x -> Arrays.asList(x.toLowerCase().replace("'", "").replace(",", "").split(" ")).iterator())
				.filter(f -> f.startsWith("-") == false);
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> {
			if (!s.startsWith("#")) {
				return new Tuple2<>((s.substring(0, 1).toUpperCase() + s.substring(1)), 1);
			} else if (s.length() > 2) {
				return new Tuple2<>((s.substring(0, 1) + s.substring(1, 2).toUpperCase() + s.substring(2)), 1);
			} else {
				return new Tuple2<>((s.substring(0, 1) + s.substring(1, 2).toUpperCase()), 1);
			}
		}).reduceByKey((i1, i2) -> i1 + i2);

		return wordCounts;

	}

	public static void main(String[] args) throws Exception {

		// String[] wordsToIgnore = {"RT"};
		JavaSparkContext sc = new JavaSparkContext("local[2]", "TwitterStream");
		sc.setLogLevel("WARN");
		JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(5000));
		jssc.checkpoint("file:///tmp/spark");
		// Add a custom receiver for RabbitMQ.
		// https://spark.apache.org/docs/2.3.1/streaming-custom-receivers.html
		JavaDStream<String> customReceiverStream = jssc.receiverStream(new CustomReceiver());

		JavaPairDStream<String, Integer> wordCounts = cleanAndReduce(customReceiverStream);
		wordCounts.foreachRDD(s -> write(s));
		// words.print();
		wordCounts.print();

		System.out.println("BEFORE start");
		jssc.start();

		jssc.awaitTermination();

	}

}