package com.dutchtulipbulb.flinkDemoProject;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamUtil {

	public static DataStream<String> getDataStream(StreamExecutionEnvironment env, ParameterTool params) {
		DataStream<String> dataStream = null;
		if (params.has("input")) {
			System.out.println("Reading from an input file");
			dataStream = env.readTextFile(params.get("input"));
		} else if (params.has("host") && params.has("port")) {
			System.out.println("reading from the socket");
			dataStream = env.socketTextStream(params.get("host"), params.getInt("port"));
		} else {
			System.out.println("Use --host and --port to read from socket");
			System.out.println("Use --input to read from a file");
		}
		return dataStream;
	}
	
}
