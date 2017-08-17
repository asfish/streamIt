package com.spark.streaming.tcp.socket;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * This is the class that connects to the server running in EventServer.java (above),
 * reads the data as it comes in and prints the data that's been received every 5 seconds.
 *
 *
 * Created by agebriel on 8/17/17.
 */
public class SimpleStreamingApp
{
	private static final String HOST = "localhost";
	private static final int PORT = 9999;

	public static void main(String[] args) {
		// Configure and initialize the SparkStreamingContext
		SparkConf conf = new SparkConf()
			.setMaster("local[*]")
			.setAppName("SimpleStreamingApp");
		JavaStreamingContext streamingContext =
			new JavaStreamingContext(conf, Durations.seconds(5));
		Logger.getRootLogger().setLevel(Level.ERROR);

		// Receive streaming data from the source
		JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream(HOST, PORT);
		lines.print();

		// Execute the Spark workflow defined above
		streamingContext.start();
		try
		{
			streamingContext.awaitTermination();
		} catch (InterruptedException e)
		{
			e.printStackTrace();
		}
	}
}
