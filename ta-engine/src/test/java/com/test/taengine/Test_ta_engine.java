package com.test.taengine;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.taengine.engine.*;


import scala.Tuple2;

class Test_ta_engine {
	@BeforeAll
	static void suiteSetUp() {
		System.out.println(DB.getDatabase(System.getProperty("user.dir")+"\\src\\test\\resources\\test.db"));
		DB.createNewDatabase(DB.getDatabase(System.getProperty("user.dir")+"\\src\\test\\resources\\test.db"));
		
	}
	
	@AfterAll
	static void suiteTearDown() {
		DB.deleteDatabaseData(DB.getDatabase(System.getProperty("user.dir")+"\\src\\test\\resources\\test.db"));
	}

	@BeforeEach
	void setUp() throws Exception {
	}

	@AfterEach
	void tearDown() throws Exception {
	}

	@Test
	void testCleanAndReduce() throws InterruptedException, IOException {
		ArrayList<String> computedPairs = new ArrayList<String>();
		ArrayList<String> expectedPairs = new ArrayList<String>();

		// fill expected pairs from file
		Scanner expectedFileScanner = new Scanner(new File("src\\test\\resources\\test_expected_pairs.txt"));
		while (expectedFileScanner.hasNext()) {
			expectedPairs.add(expectedFileScanner.nextLine());
		}
		expectedFileScanner.close();

		JavaSparkContext sc = new JavaSparkContext("local[2]", "TwitterStream");
		sc.setLogLevel("WARN");
		JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(2000));
		JavaPairDStream<String, Integer> wordCounts = Engine
				.cleanAndReduce(jssc.textFileStream("src\\test\\resources\\"));
		wordCounts.foreachRDD(s -> {
			Iterator<Tuple2<String, Integer>> sIter = s.toLocalIterator();
			System.out.println(sIter.hasNext());
			while (sIter.hasNext()) {
				computedPairs.add(sIter.next().toString());
			}
		});
		jssc.start();
		jssc.awaitTerminationOrTimeout(5000);

		// Need to make new file. Spark will not process existing files. Add test tweet bodies to file
		FileReader fr = new FileReader(new File("src\\test\\resources\\test-small2.txt"));
		int i;
		String j = "";
		while ((i = fr.read()) != -1)
			j = j + (char) i;

		// +LocalDateTime.now().toString().replace(".", "").replace(":", "") + "
		String fName = "src\\test\\resources\\test-small-2.txt";
		File f = new File(fName);
		f.createNewFile();
		PrintWriter pw = new PrintWriter(fName);
		pw.write(j);
		pw.close();
		
		// Wait for spark to start and compute the batch
		while (computedPairs.isEmpty()) {
			Thread.sleep(2000);
		}
		jssc.close();
		sc.close();
		Collections.sort(computedPairs);
		Collections.sort(expectedPairs);
		assertTrue(computedPairs.equals(expectedPairs));
		System.out.println("testCleanAndReduce finished");
	}
	
	@Test
	void testWriteToDB() {
		JavaSparkContext jsc = new JavaSparkContext("local[2]", "TwitterStream");
		
		Tuple2<String, Integer> t1 = new Tuple2<String, Integer>("#Marian", 1);
		Tuple2<String, Integer> t2 = new Tuple2<String, Integer>("The", 7);
		Tuple2<String, Integer> t3 = new Tuple2<String, Integer>("Ireland", 2);
		Tuple2<String, Integer> t4 = new Tuple2<String, Integer>("Pro-eu", 1);
		
		List<Tuple2<String, Integer>> listOfMessages = Arrays.asList(t1,t2,t3,t4);
		JavaRDD<Tuple2<String, Integer>> jrdd = jsc.parallelize(listOfMessages);
		JavaPairRDD<String, Integer> pairRdd = JavaPairRDD.fromJavaRDD(jrdd);
		
		pairRdd.foreach(f ->Engine.write(f, System.getProperty("user.dir")+"\\src\\test\\resources\\test.db", "words"));
		
		String url = DB.getDatabase(System.getProperty("user.dir")+"\\src\\test\\resources\\test.db");
	        Connection conn = null;
	        try {
	            conn = DriverManager.getConnection(url);
	        } catch (SQLException e) {
	            System.out.println(e.getMessage());
	        }
		// Read from the db and check the data is expected
		Map<String, Integer> expectedWords = new HashMap<String, Integer>();
		expectedWords.put("Ireland", 2);
		expectedWords.put("#Marian", 1);
		expectedWords.put("Pro-eu", 1);
		expectedWords.put("The", 7);
		
		String sql = "SELECT * FROM words";
        try (Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)){

        	System.out.println("Results: ");
            while (rs.next()) {
            	if(expectedWords.get(rs.getString("word")).equals(rs.getInt("count"))){
            		expectedWords.remove(rs.getString("word"));
            	}
            	else {
            		fail("Word not expected");
            	}
                System.out.println(rs.getInt("id") +  "\t" + 
                                   rs.getString("word") + "\t" +
                                   rs.getInt("count") + "\t" +
                                   rs.getString("datetime"));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        // Values removed from map if they were expected
        assertTrue(expectedWords.isEmpty());	
	}
}
