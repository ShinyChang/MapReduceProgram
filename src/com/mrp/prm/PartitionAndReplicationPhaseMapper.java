package com.mrp.prm;

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

public class PartitionAndReplicationPhaseMapper extends DefaultMapper<QuadTextPair> {
	private List<String> column;
	private List<String> join;

	private final List<Integer> foreignKeyIndex = new ArrayList<Integer>();
	private final List<String> foreignKey = new ArrayList<String>();

	private BloomFilter<Text>[] bloomFilter;
	private final List<String> bloomFilterMapping = new ArrayList<String>();
	private final List<Integer> thetaJoinTableIndex = new ArrayList<Integer>();
	private int[] RF;
	private final String EMPTY = "";

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		readTableIndex(context);
		Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());

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
					String[] tmp = joinCondition.split(op);
					String fkName = tmp[0].trim();
					String pkTableTitle = tmp[1].trim().substring(0, 1);
					if (!foreignKey.contains(fkName)) {
						foreignKey.add(fkName);
					}
					// 不是EQUAL JOIN的時候加入TABLEINDEX
					if (!op.equals(OP[5])) {
						for (int i = 0; i < DIMENSION_TABLE_INDEX.length; i++) {
							if (DIMENSION_TABLE_INDEX[i].indexOf(pkTableTitle) == 0) {
								thetaJoinTableIndex.add(i);
							}
						}
						break;
					}
				}
			}
		}

		// 讀取BloomFilter檔案
		bloomFilter = new BloomFilter[localFiles.length - 3];// without
																// part-r-00000
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
		doFactTable(context, value);
	}

	protected void doFactTable(Context context, Text value) throws IOException, InterruptedException {
		String[] columnValue = readRow(value, VERTICALBAR);
		int bf_idx;

		// BloomFilter 快速篩選
		filter: for (String fk : foreignKey) {
			
			//FIXLATER 過濾Theta join的表格(利用foreignkey的特性)
			for (int thetaindex : thetaJoinTableIndex) {
				if (fk.indexOf(DIMENSION_TABLE_INDEX[thetaindex].substring(0, 1)) == 3) {
					continue filter;
				}
			}
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
						} else {
							continue filter;
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
							new Text(EMPTY));

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
