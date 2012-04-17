package com.mrp.object;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DefaultMapper extends Mapper<Object, Text, Text, Text> {
	protected final String VERTICALBAR = "|";
	protected final String TAB = "\t";

	protected List<String> readLocalFile(Path localFiles) throws IOException {
		List<String> tmpList = new ArrayList<String>();
		FileReader fr = new FileReader(localFiles.toString());
		BufferedReader br = new BufferedReader(fr);
		while (br.ready()) {
			tmpList.add(br.readLine());
		}
		br.close();
		return tmpList;
	}

	protected String[] readRow(Text row, String splitSign) {
		StringTokenizer itr = new StringTokenizer(row.toString(), splitSign);
		int tableLength = itr.countTokens();
		String[] tmpToken = new String[tableLength];
		for (int i = 0; i < tableLength; i++) {
			tmpToken[i] = itr.nextToken();
		}
		return tmpToken;
	}
}
