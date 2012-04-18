package com.mrp.shared;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.mrp.object.DefaultReducer;

public class FinalPhaseReducer extends DefaultReducer<Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		long sum = 0;
		for (Text v : values) {
			sum += Long.parseLong(v.toString());
		}
		context.write(key, new Text(String.valueOf(sum)));
	}
}