package com.dutchtulipbulb.flinkDemoProject;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RoundUpUsingMap {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Long> dataStream = env.socketTextStream("localhost", 9999)
				.filter(new Filter()) //preserve only valid numbers
				.map(new Round()); //round every number to the nearest integer.
		
		dataStream.print(); //sink
		env.execute("Modulo Operator"); //kickoff trigger
	}
	
	public static class Filter implements FilterFunction<String> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public boolean filter(String value) throws Exception {
			try {
				Double.parseDouble(value);
				return true;
			} catch (Exception ex) {
				
			}
			return false;
		}
		
	}
	
	//<Input, Output>
	public static class Round implements MapFunction<String, Long> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public Long map(String value) throws Exception {
			//input is string and output is long
			double number = Double.parseDouble(value.trim());
			return Math.round(number);
		}
		
	}
	
}
