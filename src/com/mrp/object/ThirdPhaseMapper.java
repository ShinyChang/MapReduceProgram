package com.mrp.object;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ThirdPhaseMapper extends Mapper<Object, Text, Text, Text> {
	
	protected String[] readRow(Text row, String splitSign) {
		StringTokenizer itr = new StringTokenizer(row.toString(), splitSign);
		int tableLength = itr.countTokens();
		String[] tmpToken = new String[tableLength];
		for (int i = 0; i < tableLength; i++) {
			tmpToken[i] = itr.nextToken();
		}
		return tmpToken;	
	}

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] columnValue = readRow(value, "\t");
		StringBuffer sb = new StringBuffer();
		for (int i = 1; i < columnValue.length; i++) {
			sb.append(columnValue[i]);
			sb.append("\t");
		}
		sb.delete(sb.length() - 1, sb.length());// delete tab
		context.write(new Text(columnValue[0]), new Text(sb.toString()));
	}
}
