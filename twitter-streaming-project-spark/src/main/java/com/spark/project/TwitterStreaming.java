/**
 * 
 */
package com.spark.project;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import scala.Tuple3;
import twitter4j.Status;

/**
 * created by ronneyismael
 */

public class TwitterStreaming {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub.

		Properties prop = new Properties();
		InputStream input = null;

		input = new FileInputStream("src/main/resources/config.properties");

		// load a properties file
		prop.load(input);

		// get the property value and print it out
		String consumerKey = prop.getProperty("consumerKey");
		String consumerSecret = prop.getProperty("consumerSecret");
		String accessToken = prop.getProperty("accessToken");
		String accessTokenSecret = prop.getProperty("accessTokenSecret");
		String filePath = prop.getProperty("filePath");
		String[] filters = new String[] { prop.getProperty("filter") };

		final File file = new File(filePath);

		SparkConf sparkConfig = new SparkConf().setMaster("local[2]").setAppName("Spark Streaming - Twitter");
		// 1 minute streaming window
		JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConfig, new Duration(60000));
		System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
		System.setProperty("twitter4j.oauth.accessToken", accessToken);
		System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

		JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(javaStreamingContext, filters);

		//     Extract the text from tweet and print first 10 tweets and create a new DStream with
//        text from each tweet
		JavaDStream<String> tweets = twitterStream.map((Function<Status, String>) Status::getText);
		tweets.print();






		// split tweet stream into word stream (split a tweet into words)
		JavaDStream<String> words = tweets.flatMap(l -> Arrays.asList(l.split(" ")));
		JavaDStream<String> hashTags = words.filter((Function<String, Boolean>) word -> word.startsWith("#"));
		hashTags.print();

		JavaDStream<Tuple3<String, Integer, Integer>> tweetTotals =
				tweets
						.map(t -> new Tuple3<>(t, t.length(), t.split(" ").length));

		tweetTotals.window(Durations.seconds(3000), Durations.seconds(3000));

		tweetTotals.print();


		tweetTotals.foreachRDD(new Function<JavaRDD<Tuple3<String, Integer, Integer>>, Void>() {
			@Override
			public Void call(JavaRDD<Tuple3<String, Integer, Integer>> tuple3JavaRDD) throws Exception {
				if(tuple3JavaRDD.count()>0){
					double avgChars = tuple3JavaRDD.mapToDouble(tc -> tc._2()).mean();
					System.out.println("Average Characters : " + avgChars);
				}
				return null;
			}
		});

		tweetTotals.print();



		tweetTotals.window(Durations.seconds(3000),Durations.seconds(3000))
				.foreachRDD(new Function<JavaRDD<Tuple3<String, Integer, Integer>>, Void>() {
			@Override
			public Void call(JavaRDD<Tuple3<String, Integer, Integer>> tuple3JavaRDD) throws Exception {
				if(tuple3JavaRDD.count()>0){
					double avgChars = tuple3JavaRDD.mapToDouble(tc -> tc._2()).mean();
					System.out.println("Average Characters : " + avgChars);
				}
				return null;
			}
		});

		tweetTotals.print();






		JavaPairDStream<String, Long> hashtagsCountsSorted = hashTags.countByValue()
				.mapToPair(Tuple2::swap)
				.transformToPair(hcswap->hcswap.sortByKey(false))
				.mapToPair(Tuple2::swap);

		hashtagsCountsSorted.print(10);










		javaStreamingContext.start();
		javaStreamingContext.awaitTermination();

	}

}
