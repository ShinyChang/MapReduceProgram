package com.mrp.prm;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.mrp.object.DefaultReducer;
import com.mrp.object.QuadTextPair;

public class PartitionAndReplicationPhaseReducer extends DefaultReducer<QuadTextPair, Text> {
	List<Integer> DimensionTableKey = new ArrayList<Integer>();
	List<Integer> FactTableKey = new ArrayList<Integer>();
	List<String> DimensionTableValue = new ArrayList<String>();
	List<String> FactTableValue = new ArrayList<String>();
	int type = -1;
	final String TAB = "\t";
	final String DIMENSION_TABLE_SIGN = "D";
	final String FACT_TABLE_SIGN = "F";
	final String COMMA = ",";
	final String WHITE_SPACE = " ";
	final String EMPTY = "";
	int COUNT_OF_TABLE = -1;
	List<Integer>[] TABLE_KEY;
	List<String>[] TABLE_VALUE;
	List<String> TABLE_MAP = new ArrayList<String>();

	// initial, only do once
	public void setup(Context context) {

		// read distributed catch file
		try {
			Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			COUNT_OF_TABLE = readLocalFile(localFiles[1]).size();

			// init
			TABLE_KEY = new ArrayList[COUNT_OF_TABLE];
			TABLE_VALUE = new ArrayList[COUNT_OF_TABLE];
			for (int i = 0; i < TABLE_KEY.length; i++) {
				TABLE_KEY[i] = new ArrayList<Integer>();
				TABLE_VALUE[i] = new ArrayList<String>();
			}

			// save to global List
			FileReader fr = new FileReader(localFiles[localFiles.length - 1].toString());// part-r-00000
			BufferedReader br = new BufferedReader(fr);
			String tmp[];
			while (br.ready()) {
				tmp = br.readLine().split(TAB); // pki i jc "" col
				if (!TABLE_MAP.contains(tmp[1])) {
					TABLE_MAP.add(tmp[1]);
				}
				TABLE_KEY[TABLE_MAP.indexOf(tmp[1])].add(Integer.parseInt(tmp[0]));// pki
				if (tmp.length < 5) {
					TABLE_VALUE[TABLE_MAP.indexOf(tmp[1])].add(EMPTY);// col
				} else {
					TABLE_VALUE[TABLE_MAP.indexOf(tmp[1])].add(tmp[4]);// col
				}
			}

			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void writeOutput(Context context, int table_idx, int tableKeyIndex, Text value) throws IOException,
			InterruptedException {
		String rDi = TABLE_VALUE[table_idx].get(tableKeyIndex);
		String[] tmpArray = value.toString().split(COMMA + WHITE_SPACE);
		StringBuffer key_sb = new StringBuffer();
		StringBuffer val_sb = new StringBuffer();

		// without rf
		for (int j = 0; j < tmpArray.length - 1; j++) {
			key_sb.append(tmpArray[j]);
			key_sb.append(COMMA + WHITE_SPACE);
		}
		// delete ", "
		key_sb.delete(key_sb.length() - 2, key_sb.length());

		// column set start
		val_sb.append(table_idx);
		val_sb.append(TAB);
		// column set end

		if (rDi.equals(EMPTY)) {
			val_sb.append(tmpArray[tmpArray.length - 1]);
		} else {
			val_sb.append(rDi);
			val_sb.append(TAB);
			val_sb.append(tmpArray[tmpArray.length - 1]);
		}
		context.write(new Text(key_sb.toString()), new Text(val_sb.toString()));
	}

	public void reduce(QuadTextPair key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		int type = Integer.parseInt(key.getJoinCondition().toString());
		int table_idx = TABLE_MAP.indexOf(key.getIndex().toString());

		switch (type % 6) {
		case 0:// >=
			for (Text v : values) {
				for (int i = 0; i < TABLE_KEY[table_idx].size(); i++) {
					if (TABLE_KEY[table_idx].get(i).compareTo(Integer.parseInt(key.getKey().toString())) <= 0) {
						writeOutput(context, table_idx, i, v);
					}
				}
			}
			break;
		case 1:// <=
			for (Text v : values) {
				for (int i = 0; i < TABLE_KEY[table_idx].size(); i++) {
					if (TABLE_KEY[table_idx].get(i).compareTo(Integer.parseInt(key.getKey().toString())) >= 0) {
						writeOutput(context, table_idx, i, v);
					}
				}
			}
			break;
		case 2:// >
			for (Text v : values) {
				for (int i = 0; i < TABLE_KEY[table_idx].size(); i++) {
					if (TABLE_KEY[table_idx].get(i).compareTo(Integer.parseInt(key.getKey().toString())) < 0) {
						writeOutput(context, table_idx, i, v);
					}
				}
			}
			break;
		case 3:// <
			for (Text v : values) {
				for (int i = 0; i < TABLE_KEY[table_idx].size(); i++) {
					if (TABLE_KEY[table_idx].get(i).compareTo(Integer.parseInt(key.getKey().toString())) > 0) {
						writeOutput(context, table_idx, i, v);
					}
				}
			}
			break;
		case 4:// !=
			for (Text v : values) {
				for (int i = 0; i < TABLE_KEY[table_idx].size(); i++) {
					if (TABLE_KEY[table_idx].get(i).compareTo(Integer.parseInt(key.getKey().toString())) != 0) {
						writeOutput(context, table_idx, i, v);
					}
				}
			}
			break;
		case 5:// =
			for (Text v : values) {
				for (int i = 0; i < TABLE_KEY[table_idx].size(); i++) {
					if (TABLE_KEY[table_idx].get(i).compareTo(Integer.parseInt(key.getKey().toString())) == 0) {
						writeOutput(context, table_idx, i, v);
					}
				}
			}
			break;
		}

	}
}
