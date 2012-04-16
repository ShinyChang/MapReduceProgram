package com.mrp.object;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinalPhaseMapper extends Mapper<Object, Text, Text, Text> {
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

		// without RF
		for (int i = 0; i < columnValue.length - 1; i++) {
			sb.append(columnValue[i]);
			sb.append("\t");
		}
		sb.delete(sb.length() - 1, sb.length());// delete tab
		
		//FIXLATER 目前最少要有兩個欄位，最後為SUM運算
		context.write(new Text(sb.toString()), new Text(
				columnValue[columnValue.length - 1]));
	}
}
