package com.dutchtulipbulb.flinkDemoProject;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class ViewsSessionWindow {

	/*try to see within session which page a user spends most time on*/
	public static void main (String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> dataStream = StreamUtil.getDataStream(env, params);
		if (dataStream == null) {
			System.exit(1);
			return;
		}
		/* input entity - <user ID, page, time spent> */
		DataStream<Tuple3<String, String, Double>> outStream = dataStream.
		map (new RowSplitter())
		.keyBy(0, 1)
		.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
		.max(2); //filters only the max from the keyed stream
		
		outStream.print();
		
		env.execute("Session Window");
	}
	
	public static class RowSplitter implements MapFunction <String, Tuple3<String, String, Double>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public Tuple3<String, String, Double> map(String value) throws Exception {
			String[] fields = value.split(",");
			return new Tuple3<String, String, Double>(
					fields[0], //user ID
					fields[1], //page ID
					Double.parseDouble(fields[2])); //viewing time in minutes
		}
		
	}
			
}
