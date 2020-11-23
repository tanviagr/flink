package com.dutchtulipbulb.flinkDemoProject;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AverageViewsUsingReduce {
	/*
	 * Using this class to calculate average view time on a page on a website.
	 * Every Input entity has the name of the webpage and the time the user spends on that webpage.
	 * */
	public static void main (String[] args) throws Exception {
		
		ParameterTool params = ParameterTool.fromArgs(args);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStream<String> dataStream = StreamUtil.getDataStream(env, params);
		if (dataStream == null) {
			System.exit(1);
			return;
		}
		
		/*
		 * Every input entity has a webpage and the number of minutes a user spends on that webpage.
		 * The map() operation performed by the row splitter class splits the Row into a webpage ID, 
		 * which is the first parameter - view time of the webpage in minutes, and the count of 1 associated with
		 * every view of every webpage
		 * 
		 * This datastream is then converted into a keyed stream by the keyBy(0) operator. 
		 * Here the key is the webpage ID because we want average view time for a webpage 
		 * 
		 * We apply the reduce() operation to this keyed stream - Think of reduce as a general purpose operation.
		 * It allows you to perform any kind of aggregation on the streaming data.
		 * Here in reduce, we track the runningSum and count for each time the webpage is viewed.
		 * <webpage ID, total time, total count>
		 * 
		 * map(new Average()) - find the average number of minutes spent on every webpage.
		 * */
		
		DataStream<Tuple2<String, Double>> averageViewsStream = dataStream
		.map(new RowSplitter())
		.keyBy(0)
		.reduce(new SumAndCount())
		.map(new Average());
		
		averageViewsStream.print();
		
		env.execute("Average Views");

	}
	
	public static class RowSplitter implements MapFunction<String, Tuple3<String, Double, Long>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		//it takes in a string and maps it into <webpageID, view time, count>
		//an input row in the webpageID and the amount of time a user spends on that webpage.
		public Tuple3<String, Double, Long> map(String value) throws Exception {
			String[] fields = value.split(",");
			if (fields.length == 2) {
				return new Tuple3<String, Double, Long>(
						fields[0], /* webpage ID */
						Double.parseDouble(fields[1]), /* View Time */
						Long.valueOf(1)); /* allows us to count how many users view the webpage*/
			} else return null;
		}
		
	}
	
	public static class SumAndCount implements ReduceFunction<Tuple3<String, Double, Long>> {

		//it takes a tuple3 and produces a tuple3 - does not change the datatype it works on
		
		//reduce takes two inputs - a cumulative value and a new input
		//cumulative value holds the result of the op that we have carried on the stream so far
		//the new input is the new entity that we want to add to the cumulative result.
		//reduce operation works on a per key basis.
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public Tuple3<String, Double, Long> reduce(Tuple3<String, Double, Long> cumulative,
				Tuple3<String, Double, Long> input) throws Exception {
			
			return new Tuple3<String, Double, Long> (
					input.f0,
					input.f1 + cumulative.f1,
					input.f2 + cumulative.f2);
			
		}
		
		
	}

	public static class Average implements MapFunction<Tuple3<String, Double, Long>, Tuple2<String, Double>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public Tuple2<String, Double> map(Tuple3<String, Double, Long> input) throws Exception {
			return new Tuple2<String, Double> (
					input.f0,
					input.f1 / input.f2);
		}
		
	}
}


