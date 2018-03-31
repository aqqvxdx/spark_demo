package top.wisely.springsparkcassandra.config;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SparkStream {

	public static void main(String[] args) {
		
		SparkConf config =new SparkConf();
		config.setAppName("stream").setMaster("local[3]");
	//	boolean traceEnabled = config.isTraceEnabled();
		JavaSparkContext jsk=new JavaSparkContext(config);
		jsk.setLogLevel("ERROR");
		JavaStreamingContext jstream =new JavaStreamingContext(jsk,Durations.seconds(1));
		JavaReceiverInputDStream<String> line = jstream.socketTextStream("192.168.43.110", 9999);
		JavaDStream<String> words = line.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterable<String> call(String arg0) throws Exception {
				return	Arrays.asList(arg0.split(" "));
			}
		});
		
		JavaPairDStream<String, Integer> pair = words.mapToPair( new PairFunction<String,String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String arg0) throws Exception {
				return new Tuple2<String, Integer>(arg0, 1);
			}
		});
		
		JavaPairDStream<String, Integer> reduceByKey = pair.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0+arg1;
			}
		});
		reduceByKey.foreachRDD(new Function2<JavaPairRDD<String, Integer>, Time, Void>() {
			@Override
			public Void call(JavaPairRDD<String, Integer> rdd, Time time)
					throws Exception {
				 String counts = "Counts at time " + time + " " + rdd.collect();
				 System.out.println(counts);
				
			//	 System.out.println("Appending to " + outputFile.getAbsolutePath());
				// Files.append(counts + "\n", outputFile, Charset.defaultCharset());
				return null;
			}
		
		});
		pair.print();
		//reduceByKey.print();
		jstream.start();
		jstream.awaitTermination();
	}
}
