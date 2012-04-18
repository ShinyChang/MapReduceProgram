package com.mrp.shared;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.mrp.object.DefaultMapper;

public class FinalPhaseMapper extends DefaultMapper<Text> {

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] columnValue = readRow(value, TAB);
		StringBuffer sb = new StringBuffer();

		// without RF
		for (int i = 0; i < columnValue.length - 1; i++) {
			sb.append(columnValue[i]);
			sb.append(TAB);
		}
		sb.delete(sb.length() - 1, sb.length());// delete tab

		// FIXLATER 目前最少要有兩個欄位，最後為SUM運算
		context.write(new Text(sb.toString()), new Text(columnValue[columnValue.length - 1]));
	}
}
