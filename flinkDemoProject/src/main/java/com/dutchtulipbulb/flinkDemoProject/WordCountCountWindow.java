package com.dutchtulipbulb.flinkDemoProject;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountCountWindow {

	/*
	 * 
	 * Every time the count of some key reaches 3 - it is outputted according to the toString()
	 * method defined in the POJO class - relevant because earlier we were printing the Tuples 
	 * and they have internal implementations of the tostring() method.
	 * 
	 * */
	public static void main (String[] args) throws Exception {
		/*Count window counts on a per key basis - only works on keyed streams.*/
		ParameterTool params = ParameterTool.fromArgs(args);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStream<String> dataStream = StreamUtil.getDataStream(env, params);
		if (dataStream == null) {
			System.exit(1);
			return;
		}
		/* the keyBy() operation takes in a string rather than an index.
		 * because the result of the flatMap produces not a Tuple, but a java object.
		 * the keyBy() function can take in a string which is a member variable of this object.
		 * */
		
		DataStream<WordCount> outStream = dataStream
		.flatMap(new WordCountSplitter())
		.keyBy("word")
		.countWindow(3)
		.sum("count"); //irrelevant - every time the count of the object reaches 3 it gets printed.
		
		outStream.print();
		env.execute("Count Window");
		
	}
	
	public static class WordCountSplitter implements FlatMapFunction<String, WordCount> {

		/**
		 * WordCount is a POJO (plain old java object) class 
		 * that needs to have a default constructor. It can have other constructors- but it 
		 * needs a default one explicitly.
		 * 
		 * the member variables of this object that hold data should be public or should have
		 * getters and setters that are public. 
		 */
		private static final long serialVersionUID = 1L;

		//produces a java object as a result.
		public void flatMap(String value, Collector<WordCount> out) throws Exception {
			
			for (String word : value.split(" " )) {
				out.collect(new WordCount(word, 1));
			}
			
		}
		
	}

}
