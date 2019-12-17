package com.taengine.engine;

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
import scala.Tuple2;

public class Engine {
	public static void write(Tuple2<String, Integer> jds, String dbName, String table){

		String url = DB.getDatabase(dbName);
		Connection conn = null;

		try {
			conn = DriverManager.getConnection(url);
			// Does the word have to be updated or inserted
			String sqlSelectStr = "SELECT * FROM " + table + " WHERE word=" + "'" + jds._1 + "'";
			try (Statement stmt = conn.createStatement(); 
					ResultSet rs = stmt.executeQuery(sqlSelectStr)) 
			{
				if (rs.next()) {
					stmt.close();
					// Do an update
					String sqlUpdateStr = "UPDATE "+table+" SET count = count + ?, datetime = ? WHERE word = ?";
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
		JavaDStream<String> words = jds
				.flatMap(x -> Arrays.asList(x.toLowerCase().replace("'", "").replace(",", "").split(" ")).iterator())
				.filter(f -> f.startsWith("-") == false);
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> {
			if (!s.startsWith("#") & s.length()>1) {
				return new Tuple2<>((s.substring(0, 1).toUpperCase() + s.substring(1)), 1);
			}
			else if (!s.startsWith("#")) {
				return new Tuple2<>((s.toUpperCase()), 1);
			}
			else if (s.length() > 2) {
				return new Tuple2<>((s.substring(0, 1) + s.substring(1, 2).toUpperCase() + s.substring(2)), 1);
			} else {
				return new Tuple2<>((s.substring(0, 1) + s.substring(1, 2).toUpperCase()), 1);
			}
		}).reduceByKey((i1, i2) -> i1 + i2);

		return wordCounts;

	}

	public static void main(String[] args) throws Exception {

		// String[] wordsToIgnore = {"RT"};
		DB.createNewDatabase(DB.getDatabase(System.getProperty("user.dir")+"\\src\\main\\resources\\db_1.db"));
		JavaSparkContext sc = new JavaSparkContext("local[2]", "TwitterStream");
		sc.setLogLevel("WARN");
		JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(5000));
		jssc.checkpoint("file:///tmp/spark");
		// Add a custom receiver for RabbitMQ.
		// https://spark.apache.org/docs/2.3.1/streaming-custom-receivers.html
		JavaDStream<String> customReceiverStream = jssc.receiverStream(new CustomReceiver());

		JavaPairDStream<String, Integer> wordCounts = cleanAndReduce(customReceiverStream);
		wordCounts.foreachRDD(s -> {
			s.foreach(f -> write(f, System.getProperty("user.dir")+"\\src\\main\\resources\\db_1.db", "words"));
		});
		// words.print();
		wordCounts.print();

		System.out.println("BEFORE start");
		jssc.start();

		jssc.awaitTermination();

	}

}