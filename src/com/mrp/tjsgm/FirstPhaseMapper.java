package com.mrp.tjsgm;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.mrp.lib.ConditionValidator;
import com.mrp.object.QuadMapper;
import com.mrp.object.QuadTextPair;

public class FirstPhaseMapper extends QuadMapper {
	private List<String> column;
	private List<String> filter;

	ConditionValidator conditionValidator = new ConditionValidator();

	public void setup(Context context) throws IOException {
		readTableIndex(context);

		Path[] localFiles = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());
		column = readLocalFile(localFiles[0]);
		filter = readLocalFile(localFiles[1]);
		conditionValidator.loadFilter(filter);

		// context, join
		readJoinIndex(context, readLocalFile(localFiles[3]));
	}

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		// 讀取一列轉換為欄位陣列
		String[] columnValue = readRow(value, "|");

		// 建立Key <primaryKey, tableIndex, joinCondition>
		outputKey = new QuadTextPair(new IntWritable(
				Integer.parseInt(columnValue[0])), new IntWritable(tableIndex),
				new IntWritable(joinIndex), new Text("D"));
		boolean result = false;

		// find column & check it
		for (String c : filter) {
			for (int i = 0; i < SCHEMA[tableIndex].length; i++) {
				if (c.contains(SCHEMA[tableIndex][i])) {
					result = conditionValidator.valid(tableIndex, i,
							columnValue[i]);
				}
			}
		}
		if (result) {
			// 建立Value <PK, PK, PK, ..., PK>
			StringBuffer sb = new StringBuffer();
			for (String c : column) {// match column
				for (int i = 0; i < SCHEMA[tableIndex].length; i++) {
					if (c.equals(SCHEMA[tableIndex][i])) {
						sb.append(columnValue[i]);
						sb.append(", ");
					}
				}
			}
			if (sb.length() > 2) {
				outputValue.set(sb.substring(0, sb.length() - 2));
			}
			context.write(outputKey, outputValue);
		}
	}
}