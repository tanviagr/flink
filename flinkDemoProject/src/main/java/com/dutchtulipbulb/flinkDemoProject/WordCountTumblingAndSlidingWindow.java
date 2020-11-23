package com.dutchtulipbulb.flinkDemoProject;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCountTumblingAndSlidingWindow {

	/*Apply sum operation on word counts within the window*/
	public static void main (String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStream<String> dataStream = StreamUtil.getDataStream(env, params);
		if (dataStream == null) {
			System.exit(1);
			return;
		}
		//We want to use Tumbling Window and processing time as the notion of time 
		
		/* ------ TUMBLING WINDOW ------ */
		/*
		 * DataStream<Tuple2<String, Long>> outputStream = dataStream
		.flatMap(new WordCountSplitter())
		.keyBy(0)
		.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
		.sum(1);
		*/
		
		// apply sum aggregation over the entities in that window 
		// for every word we want to see how often it occurs in a 10 second window. 
		
		DataStream<Tuple2<String, Long>> outputStream = dataStream
				.flatMap(new WordCountSplitter())
				.keyBy(0)
				.window(SlidingProcessingTimeWindows.of(Time.seconds(20), Time.seconds(10)))
				.sum(1);
		/* sliding interval is how much overlap exists between windows */
		
		outputStream.print();
		
		env.execute("Tumbling and Sliding Window"); //lazy loading
	}
	
	public static class WordCountSplitter implements FlatMapFunction<String, Tuple2<String, Long>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
			for (String word : value.split(" ")) {
				out.collect(new Tuple2<String, Long>(
						word,
						Long.valueOf(1)));
			}
		}
		
	}
	
}
