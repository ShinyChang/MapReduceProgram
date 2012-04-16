package com.mrp.object;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinalPhaseReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		long sum = 0;
		for (Text v : values) {
			sum += Long.parseLong(v.toString());
		}
		context.write(key, new Text(String.valueOf(sum)));
	}
}