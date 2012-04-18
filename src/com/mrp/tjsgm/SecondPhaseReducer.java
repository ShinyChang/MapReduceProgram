package com.mrp.tjsgm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;

import com.mrp.object.DefaultReducer;
import com.mrp.object.QuadTextPair;

public class SecondPhaseReducer extends DefaultReducer<QuadTextPair, Text> {
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

	private void writeOutput(Context context, int dimensionTableKey) throws IOException, InterruptedException {
		String FTVtmp = FactTableValue.get(dimensionTableKey);
		context.write(
				new Text(FTVtmp.substring(0, FTVtmp.lastIndexOf(COMMA + WHITE_SPACE))),

				// FIXLATER 目前根據欄位給予編號(第三階段會使用)
				new Text((type / 6)// table id
						+ TAB
						+ DimensionTableValue.get(dimensionTableKey)// Rdi
						+ FTVtmp.substring(
								FTVtmp.lastIndexOf(COMMA + WHITE_SPACE) + COMMA.length() + WHITE_SPACE.length(),
								FTVtmp.length())));
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {

		switch (type % 6) {
		case 0:// >=
			for (int i = 0; i < FactTableKey.size(); i++) {
				for (int j = 0; j < DimensionTableKey.size(); j++) {
					if (FactTableKey.get(i) >= DimensionTableKey.get(j)) {
						writeOutput(context, j);
					}
				}
			}
			break;
		case 1:// <=
			for (int i = 0; i < FactTableKey.size(); i++) {
				for (int j = 0; j < DimensionTableKey.size(); j++) {
					if (FactTableKey.get(i) <= DimensionTableKey.get(j)) {
						writeOutput(context, j);
					}
				}
			}
			break;
		case 2:// >
			for (int i = 0; i < FactTableKey.size(); i++) {
				for (int j = 0; j < DimensionTableKey.size(); j++) {
					if (FactTableKey.get(i) > DimensionTableKey.get(j)) {
						writeOutput(context, j);
					}
				}
			}
			break;
		case 3:// <
			for (int i = 0; i < FactTableKey.size(); i++) {
				for (int j = 0; j < DimensionTableKey.size(); j++) {
					if (FactTableKey.get(i) < DimensionTableKey.get(j)) {
						writeOutput(context, j);
					}
				}
			}
			break;
		case 4:// !=
			for (int i = 0; i < FactTableKey.size(); i++) {
				for (int j = 0; j < DimensionTableKey.size(); j++) {
					if (FactTableKey.get(i) != DimensionTableKey.get(j)) {
						writeOutput(context, j);
					}
				}
			}
			break;
		}
	}

	@Override
	public void reduce(QuadTextPair key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		type = Integer.parseInt(key.getJoinCondition().toString());
		String tmp;
		switch (type % 6) {
		case 0:// >=
		case 1:// <=
		case 2:// >
		case 3:// <
		case 4:// !=
			for (Text v : values) {
				tmp = v.toString();
				if (!tmp.contains(COMMA + WHITE_SPACE)) {
					DimensionTableKey.add(Integer.parseInt(key.getKey().toString()));
					DimensionTableValue.add(tmp);
				} else {
					FactTableKey.add(Integer.parseInt(key.getKey().toString()));
					FactTableValue.add(tmp);
				}
			}
			break;
		case 5:// =
			List<String> fk = new ArrayList<String>();
			List<String> pk = new ArrayList<String>();

			for (Text v : values) {
				tmp = v.toString();

				// FIXLATER 只有支援一個表格一個顯示欄位
				if (tmp.contains(COMMA + WHITE_SPACE)) {
					fk.add(tmp);
				} else {
					pk.add(tmp);
				}
			}

			String[] tmpArray;
			StringBuffer key_sb = new StringBuffer();
			StringBuffer val_sb = new StringBuffer();

			// 對於每個外來鍵建立一組Key-Value Pair
			for (String k : fk) {
				tmpArray = k.split(COMMA + WHITE_SPACE);
				// without rf
				for (int i = 0; i < tmpArray.length - 1; i++) {
					key_sb.append(tmpArray[i]);
					key_sb.append(COMMA + WHITE_SPACE);
				}
				// delete ", "
				if (key_sb.length() > 0) {
					key_sb.delete(key_sb.length() - 2, key_sb.length());
				}

				// column set start
				// FIXLATER 目前根據欄位給予編號(第三階段會使用)
				val_sb.append(key.getIndex());
				val_sb.append(TAB);
				// column set end

				String[] tmptmp;
				for (String v : pk) {
					tmptmp = v.split(COMMA + WHITE_SPACE);
					for (int j = 0; j < tmptmp.length; j++) {
						if (!tmptmp[j].equals(EMPTY)) {
							val_sb.append(tmptmp[j]);
							val_sb.append(TAB);
						}
					}
				}
				val_sb.append(tmpArray[tmpArray.length - 1]);// rf
				context.write(new Text(key_sb.toString()), new Text(val_sb.toString()));
				val_sb.delete(0, val_sb.length());
				key_sb.delete(0, key_sb.length());
			}
		}

	}
}