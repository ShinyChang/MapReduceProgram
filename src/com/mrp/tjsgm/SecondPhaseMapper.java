package com.mrp.tjsgm;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.mrp.object.BloomFilter;
import com.mrp.object.DefaultMapper;
import com.mrp.object.QuadTextPair;

public class SecondPhaseMapper extends DefaultMapper<QuadTextPair> {
	private final String FP_OUTPUT = "part-r-00000";

	private List<String> column;
	private List<String> join;

	private final List<Integer> foreignKeyIndex = new ArrayList<Integer>();
	private final List<String> foreignKey = new ArrayList<String>();

	private BloomFilter<Text>[] bloomFilter;
	private final List<String> bloomFilterMapping = new ArrayList<String>();
	private boolean isFirstPhaseOutput = false;
	private boolean isDimensionTable = false;
	private boolean isFactTable = false;
	private int[] RF;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		readTableIndex(context);
		Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());

		// 判斷檔案來源
		if (fileName.equals(FP_OUTPUT)) {
			isFirstPhaseOutput = true;
		} else if (tableIndex >= 0) {
			isDimensionTable = true;
		} else if (tableIndex == -1) {
			isFactTable = true;
		}

		// 讀取參數
		column = readLocalFile(localFiles[0]);
		join = readLocalFile(localFiles[1]);

		// 尋找RF (只適用於SSB的查詢)
		// FIXLATER 只有實作SUM且裡面只有相減
		for (String c : column) {
			c = c.trim();
			if (c.contains(SUM + LEFT_BRACKET)) {
				String[] tmp = c.substring(4, c.indexOf(RIGHT_BRACKET)).split(MINUS);
				RF = new int[tmp.length];
				for (int idx = 0; idx < tmp.length; idx++) {
					tmp[idx] = tmp[idx].trim();
					for (int i = 0; i < FACT_TABLE_SCHEMA.length; i++) {
						if (tmp[idx].equals(FACT_TABLE_SCHEMA[i])) {
							RF[idx] = i;
							break;
						}
					}
				}
			}
		}

		// 新增Foreign Key
		for (String joinCondition : join) {
			for (String op : OP) {
				if (joinCondition.contains(op)) {
					String tmp = joinCondition.split(op)[0].trim();
					if (!foreignKey.contains(tmp)) {
						foreignKey.add(tmp);
					}
				}
			}
		}

		// 讀取BloomFilter檔案
		bloomFilter = new BloomFilter[localFiles.length - 2];
		for (int i = 0; i < bloomFilter.length; i++) {
			bloomFilter[i] = new BloomFilter<Text>();
			bloomFilter[i].readFields(new DataInputStream(new FileInputStream(localFiles[i + 2].toString())));
			bloomFilterMapping.add(localFiles[i + 2].getName());
		}

		// Foreign key Index
		for (String fk : foreignKey) {
			for (int i = 0; i < FACT_TABLE_SCHEMA.length; i++) {
				if (FACT_TABLE_SCHEMA[i].equals(fk)) {
					foreignKeyIndex.add(i);
				}
			}
		}

		readJoinIndex(context, join);
	}

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		// 真實表格
		if (isFactTable) {
			doFactTable(context, value);
		}

		// 未經過篩選的DimensionTable的資料
		if (isDimensionTable) {
			doDimensionTable(context, value);
		}

		// 第一階段篩選過後的資料(Dimension Table)
		if (isFirstPhaseOutput) {
			doFirstPhaseOutput(context, value);
		}
		outputValue.clear();
	}

	protected void doFirstPhaseOutput(Context context, Text value) throws IOException, InterruptedException {
		String[] columnValue = readRow(value, TAB);
		outputKey = new QuadTextPair(columnValue[0], columnValue[1], columnValue[2], columnValue[3]);

		// 如果有欄位被選取(僅適用SSB)
		if (columnValue.length > 4) {
			outputValue.set(columnValue[4]);
		}
		context.write(outputKey, outputValue);
	}

	protected void doDimensionTable(Context context, Text value) throws IOException, InterruptedException {
		String[] columnValue = readRow(value, VERTICALBAR);
		outputKey = new QuadTextPair(new IntWritable(Integer.parseInt(columnValue[0])), new IntWritable(tableIndex),
				new IntWritable(joinIndex), new Text(DIMENSION_TABLE_SIGN));

		StringBuffer sb = new StringBuffer();
		for (String c : column) {// match column
			for (int i = 0; i < SCHEMA[tableIndex].length; i++) {
				if (c.equals(SCHEMA[tableIndex][i])) {
					sb.append(columnValue[i]);
					sb.append(COMMA + WHITE_SPACE);
				}
			}
		}

		// 如果是不是空的
		if (sb.length() > 2) {
			outputValue.set(sb.substring(0, sb.length() - 2));
		}
		context.write(outputKey, outputValue);
	}

	protected void doFactTable(Context context, Text value) throws IOException, InterruptedException {
		String[] columnValue = readRow(value, VERTICALBAR);
		int bf_idx;

		// BloomFilter 快速篩選
		// TODO Theta Join
		for (String fk : foreignKey) {
			for (int i = 0; i < foreignKeyIndex.size(); i++) {
				int fkIndex = foreignKeyIndex.get(i);
				String factTableColumnName = FACT_TABLE_SCHEMA[fkIndex];

				// 找到Foreign的Column
				if (fk.equals(factTableColumnName)) {
					bf_idx = bloomFilterMapping.indexOf(String.valueOf(FACT_TABLE_FOREIGN_INDEX[fkIndex]));
					if (bf_idx >= 0) {

						// 如果不存在BloomFilter裡面的話就離開
						if (!bloomFilter[bf_idx].contains(new Text(columnValue[fkIndex]))) {
							return;
						}
					}
				}
			}
		}

		// 經過BloomFilter篩選過後
		int factTableJoinIndex = -1;
		for (String fk : foreignKey) {// lo_orderdate, lo_partke, lo_suppkey
			for (int i = 0; i < foreignKeyIndex.size(); i++) {// 5, 3, 4
				int fkIndex = foreignKeyIndex.get(i);
				String factTableColumnName = FACT_TABLE_SCHEMA[fkIndex];

				// 找到Foreign的Column
				if (fk.equals(factTableColumnName)) {

					// 尋找FactTable的joinIndex
					for (int j = 0; j < join.size(); j++) {
						String dimensionTableName = DIMENSION_TABLE_INDEX[FACT_TABLE_FOREIGN_INDEX[fkIndex]];

						// 尋找對應於該DimensionTable的joinIndex
						if (join.get(j).contains(dimensionTableName.substring(0, 1) + UNDER_LINE)) {
							for (int k = 0; k < OP.length; k++) {
								if (join.get(j).contains(OP[k])) {
									factTableJoinIndex = j * 6 + k;
									break;
								}
							}
						}
					}

					// 建立Key
					outputKey = new QuadTextPair(new IntWritable(Integer.parseInt(columnValue[fkIndex])),
							new IntWritable(FACT_TABLE_FOREIGN_INDEX[fkIndex]), new IntWritable(factTableJoinIndex),
							new Text(FACT_TABLE_SIGN));

					// 建立 value<fk1, fk2, ..., fkn, RF>
					StringBuffer sb = new StringBuffer();

					// 建立Foreign Key
					for (int idx : foreignKeyIndex) {
						sb.append(columnValue[idx]);
						sb.append(COMMA + WHITE_SPACE);
					}

					// 建立RF
					// FIXLATER 只有實作相減
					if (RF.length > 1) {
						sb.append(Integer.parseInt(columnValue[RF[0]]) - Integer.parseInt(columnValue[RF[1]]));
					} else {
						sb.append(columnValue[RF[0]]);
					}

					outputValue.set(sb.toString());
					context.write(outputKey, outputValue);
				}
			}
		}
	}

}
