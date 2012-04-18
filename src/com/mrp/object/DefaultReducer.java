package com.mrp.object;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DefaultReducer<KeyIn, KeyOut> extends Reducer<KeyIn, Text, KeyOut, Text> {
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
}
