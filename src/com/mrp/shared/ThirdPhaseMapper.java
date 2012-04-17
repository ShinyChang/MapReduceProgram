package com.mrp.shared;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.mrp.object.DefaultMapper;

public class ThirdPhaseMapper extends DefaultMapper {

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] columnValue = readRow(value, TAB);
		StringBuffer sb = new StringBuffer();
		for (int i = 1; i < columnValue.length; i++) {
			sb.append(columnValue[i]);
			sb.append(TAB);
		}
		sb.delete(sb.length() - 1, sb.length());// delete tab
		context.write(new Text(columnValue[0]), new Text(sb.toString()));
	}
}
