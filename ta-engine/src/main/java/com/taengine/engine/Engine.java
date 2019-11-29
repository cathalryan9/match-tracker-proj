package com.taengine.engine;

import java.util.Arrays;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;


public class Engine{
	public static void write(JavaPairRDD<String, Integer> jds) {
		
		System.out.println("Write method");
		jds.foreachAsync(f -> System.out.println(f._1() + " : " + f._2()));
	}
	public static void main(String[] args) throws Exception {
		
		//String[] wordsToIgnore = {"RT"}; 
		JavaSparkContext sc = new JavaSparkContext("local[2]", "TwitterStream");
		sc.setLogLevel("WARN");
		JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(5000));
		jssc.checkpoint("file:///tmp/spark");
		// Add a custom receiver for RabbitMQ. https://spark.apache.org/docs/2.3.1/streaming-custom-receivers.html
		JavaReceiverInputDStream<String> customReceiverStream = jssc.receiverStream(new CustomReceiver());

		JavaDStream<String> words = customReceiverStream.flatMap(x -> Arrays.asList(x.split(" ")).iterator()).filter(x -> !x.equals("RT"));
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
		        .reduceByKey((i1, i2) -> i1 + i2);
		customReceiverStream.print();
		wordCounts.foreachRDD(s -> write(s));
		//words.print();
		wordCounts.print();
		
		System.out.println("BEFORE start");
		jssc.start();
		
		jssc.awaitTermination();
		
		
	}
	
}