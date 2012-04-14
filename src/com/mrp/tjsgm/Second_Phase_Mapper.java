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

import com.mrp.lib.ConditionValidator;
import com.mrp.object.BloomFilter;
import com.mrp.object.QuadMapper;
import com.mrp.object.QuadTextPair;

public class Second_Phase_Mapper extends QuadMapper {
	private final String FP_OUTPUT = "part-r-00000";

	private List<String> column;
	private List<String> filter;
	private List<String> join;

	private List<Integer> foreignKeyIndex = new ArrayList<Integer>();
	private List<String> foreignKey = new ArrayList<String>();

	private BloomFilter<Text>[] bloomFilter;
	private List<String> bloomFilterMapping = new ArrayList<String>();
	private ConditionValidator conditionValidator = new ConditionValidator();
	private boolean isFirstPhaseOutput = false;
	private boolean isDimensionTable = false;
	private boolean isFactTable = false;
	private int[] RF;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		readTableIndex(context);
		Path[] localFiles = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());

		// 判斷檔案來源
		if (fileName.equals(FP_OUTPUT)) {
			isFirstPhaseOutput = true;
		} else if (tableIndex >= 0) {
			isDimensionTable = true;
		} else if (tableIndex == -1) {
			isFactTable = true;
		}

		column = readLocalFile(localFiles[0]);

		// 尋找RF (只適用於SSB的查詢)
		for (String c : column) {
			c = c.trim();
			if (c.contains("sum(")) {
				String[] tmp = c.substring(4, c.indexOf(")") - 1).split("-");
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

		filter = readLocalFile(localFiles[1]);
		join = readLocalFile(localFiles[2]);

		conditionValidator.loadFilter(filter);

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
		bloomFilter = new BloomFilter[localFiles.length - 3];
		for (int i = 0; i < bloomFilter.length; i++) {
			bloomFilter[i] = new BloomFilter<Text>();
			bloomFilter[i].readFields(new DataInputStream(new FileInputStream(
					localFiles[i + 3].toString())));
			bloomFilterMapping.add(localFiles[i + 3].getName());
//			System.out.println("bloomFilterMapping:"+bloomFilterMapping);
		}

		// Foreign key Index
		for (String fk : foreignKey) {
			for (int i = 0; i < FACT_TABLE_SCHEMA.length; i++) {
				if (FACT_TABLE_SCHEMA[i].equals(fk)) {
					foreignKeyIndex.add(i);
//					System.out.println("foreignKeyIndex:"+foreignKeyIndex);
				}
			}
		}

		readJoinIndex(context, join);
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		// 真實表格
		if (isFactTable) {
			String[] columnValue = readRow(value, "|");
			int bf_idx;

			// bloomfilter 快速篩選 (只適用於Equi-join)
			for (String fk : foreignKey) {// lo_orderdate, lo_partke, lo_suppkey
//				System.out.println(fk);
				for (int i = 0; i < foreignKeyIndex.size(); i++) {// 5, 3, 4
//					System.out.println(foreignKeyIndex.get(i));
					int fkIndex = foreignKeyIndex.get(i);
					String factTableColumnName = FACT_TABLE_SCHEMA[fkIndex];

					// 找到Foreign的Column
					if (fk.equals(factTableColumnName)) {
						bf_idx = bloomFilterMapping.indexOf(String
								.valueOf(FACT_TABLE_FOREIGN_INDEX[fkIndex]));
						if (bf_idx >= 0) {

							// 如果不存在BloomFilter裡面的話就離開
							if (!bloomFilter[bf_idx].contains(new Text(
									columnValue[fkIndex]))) {
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

//					System.out.println("ftcn:"+factTableColumnName+"\tfk:"+fk);
					// 找到Foreign的Column
					if (fk.equals(factTableColumnName)) {

						// 尋找FactTable的joinIndex
						for (int j = 0; j < join.size(); j++) {
							String dimensionTableName = DIMENSION_TABLE_INDEX[FACT_TABLE_FOREIGN_INDEX[fkIndex]];

							// 尋找對應於該DimensionTable的joinIndex
//							System.out.println("dimensionTableName:"
//									+ dimensionTableName);
							if (join.get(j).contains(
									dimensionTableName.substring(0, 1) + "_")) {
								for (int k = 0; k < OP.length; k++) {
									if (join.get(j).contains(OP[k])) {
										factTableJoinIndex = j * 6 + k;
										break;
									}
								}
							}
						}
//						System.out.println("GET");

						// 建立Key
						outputKey = new QuadTextPair(new IntWritable(
								Integer.parseInt(columnValue[fkIndex])),
								new IntWritable(
										FACT_TABLE_FOREIGN_INDEX[fkIndex]),
								new IntWritable(factTableJoinIndex), new Text(
										"F"));

						// 建立 value<fk1, fk2, ..., fkn, RF>
						StringBuffer sb = new StringBuffer();

						// 建立Foreign Key
						for (int idx : foreignKeyIndex) {// 5, 3, 4
							sb.append(columnValue[idx]);
							sb.append(", ");
						}

						// 建立RF
						if (RF.length > 1) {
							sb.append(Integer.parseInt(columnValue[RF[0]])
									- Integer.parseInt(columnValue[RF[1]]));
						} else {
							sb.append(columnValue[RF[0]]);
						}

						outputValue.set(sb.toString());
						context.write(outputKey, outputValue);
					}
				}
			}

		}

		// 未經過篩選的DimensionTable的資料
		if (isDimensionTable) {
			String[] columnValue = readRow(value, "|");
			outputKey = new QuadTextPair(new IntWritable(
					Integer.parseInt(columnValue[0])), new IntWritable(
					tableIndex), new IntWritable(joinIndex), new Text("D"));

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

		// 第一階段篩選過後的資料(Dimension Table)
		if (isFirstPhaseOutput) {
//			System.out.println(fileName+"\t"+value);
			String[] columnValue = readRow(value, "\t");
			outputKey = new QuadTextPair(columnValue[0], columnValue[1],
					columnValue[2], columnValue[3]);
			if (columnValue.length > 4) {
				outputValue.set(columnValue[4]);
			}
			context.write(outputKey, outputValue);
			
		}
		outputValue.clear();
	}

}
