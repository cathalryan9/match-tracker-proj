package com.test.taengine;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import java.util.ArrayList;
import java.util.Collections;

import java.util.Iterator;
import java.util.Scanner;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.taengine.engine.*;

import scala.Tuple2;

class Test_ta_engine {

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
		JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(500));
		JavaPairDStream<String, Integer> wordCounts = Engine
				.cleanAndReduce(jssc.textFileStream("src\\test\\resources\\"));
		wordCounts.foreachRDD(s -> {
			Iterator<Tuple2<String, Integer>> sIter = s.toLocalIterator();
			while (sIter.hasNext()) {
				computedPairs.add(sIter.next().toString());
			}
		});
		jssc.start();
		jssc.awaitTerminationOrTimeout(4000);

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
			Thread.sleep(500);
		}
		Collections.sort(computedPairs);
		Collections.sort(expectedPairs);
		assertTrue(computedPairs.equals(expectedPairs));
	}

}
