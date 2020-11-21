package com.dutchtulipbulb.flinkDemoProject;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountUsingKeyedStreams {

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStream<String> inputStream = StreamUtil.getDataStream(env, params);
		if (inputStream == null) {
			System.exit(1);
			return;
		}
		
		/*
		 * 1. the flatMap() function takes an input datastream and breaks it into
		 * words and associates the count 1 with each word. <word, 1>
		 * keyBy(0) makes a logical grouping of the word and puts them into buckets
		 * sum(1) takes the bucket and adds the count associated with the words.
		 * */
		
		DataStream<Tuple2<String, Integer>> wordCountStream= inputStream
				.flatMap(new WordCountSplitter())  
				.keyBy(0)
				.sum(1);
		
		wordCountStream.print();
		
		env.execute("Word Count Operation");
	}
	
	public static class WordCountSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public void flatMap(String string, Collector<Tuple2<String, Integer>> out) throws Exception {
			for (String word : string.split(" ")) {
				out.collect(new Tuple2<String, Integer>(word, 1));
			}
			
		}
		
	}
	
}
