package com.dutchtulipbulb.flinkDemoProject;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterStringsUsingFilter {
	
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = 
				StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> dataStream = env
				.socketTextStream("localhost", 9999) //input
				.filter(new Filter()); //transformation
		
		dataStream.print(); //output - store it in a sink or print on screen
		
		env.execute(); //lazy evaluation
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
			} catch (Exception e) {
				
			}
			return false;
		}
		
	}
	
}
