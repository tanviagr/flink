package com.dutchtulipbulb.flinkDemoProject;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Words {

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		env.getConfig().setGlobalJobParameters(params);
		//allows us to see our user defined arguments in the apache flink dashboard at port 8081
		
		DataStream<String> dataStream;
		if (params.has("input")) {
			System.out.println("Reading from an input file");
			dataStream = env.readTextFile(params.get("input"));
		} else if (params.has("host") && params.has("port")) {
			System.out.println("reading from socket stream");
			dataStream = env.socketTextStream(params.get("host"), params.getInt("port"));
		} else {
			System.out.println("Use --host and --port for reading from socket");
			System.out.println("Use --input to specify file path");
			System.exit(1);
			return;
		}
		
		DataStream<String> newDataStream = dataStream.flatMap(new Splitter());
		
		newDataStream.print();
		env.execute("Word Split");
	}
	
	public static class Splitter implements FlatMapFunction<String, String> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		// input datatype and output type
		// Collector is a utility that allows you to emit a record to the stream - the input stream will be broken up into multiple
		//words and you emit each single word as a different entity using the collector
		public void flatMap(String sentence, Collector<String> out) throws Exception {
			for (String word : sentence.split(" ")) {
				out.collect(word);
			}
		}
		
		
	}
}
