package com.mrp.tjsgm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.mrp.object.QuadTextPair;

public class Second_Phase_Reducer extends
		Reducer<QuadTextPair, Text, Text, Text> {
	List<Integer> DimensionTableKey = new ArrayList<Integer>();
	List<Integer> FactTableKey = new ArrayList<Integer>();
	List<String> DimensionTableValue = new ArrayList<String>();
	List<String> FactTableValue = new ArrayList<String>();
	int type = -1;

	// initial, only do once
	@Override
	public void setup(Context context) {

	}

	// destroy, only do once
	@Override
	public void cleanup(Context context) {

		String FTVtmp;
		switch (type % 6) {
		case 0:// >=
			for (int i = 0; i < FactTableKey.size(); i++) {
				for (int j = 0; j < DimensionTableKey.size(); j++) {
					if (FactTableKey.get(i) >= DimensionTableKey.get(j)) {
						try {
							FTVtmp = FactTableValue.get(i);
							// System.out.println("FactTableValue: "+FTVtmp);
							context.write(
									new Text(FTVtmp.substring(0,
											FTVtmp.lastIndexOf(", "))),
									new Text(
											(type / 6)
													+ "\t"
													+ DimensionTableValue
															.get(j)// Rdi
													+ FTVtmp.substring(
															FTVtmp.lastIndexOf(", ") + 2,
															FTVtmp.length())));
						} catch (IOException e) {
							e.printStackTrace();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			}
			break;
		case 1:// <=
			for (int i = 0; i < FactTableKey.size(); i++) {
				for (int j = 0; j < DimensionTableKey.size(); j++) {
					if (FactTableKey.get(i) <= DimensionTableKey.get(j)) {
						try {
							FTVtmp = FactTableValue.get(i);
							// System.out.println("FactTableValue: "+FTVtmp);
							context.write(
									new Text(FTVtmp.substring(0,
											FTVtmp.lastIndexOf(", "))),
									new Text(
											(type / 6)
													+ "\t"
													+ DimensionTableValue
															.get(j)// Rdi
													+ FTVtmp.substring(
															FTVtmp.lastIndexOf(", ") + 2,
															FTVtmp.length())));
						} catch (IOException e) {
							e.printStackTrace();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			}
			break;
		case 2:// >
			for (int i = 0; i < FactTableKey.size(); i++) {
				for (int j = 0; j < DimensionTableKey.size(); j++) {
					if (FactTableKey.get(i) > DimensionTableKey.get(j)) {
						try {
							FTVtmp = FactTableValue.get(i);
							// System.out.println("FactTableValue: "+FTVtmp);
							context.write(
									new Text(FTVtmp.substring(0,
											FTVtmp.lastIndexOf(", "))),
									new Text(
											(type / 6)
													+ "\t"
													+ DimensionTableValue
															.get(j)// Rdi
													+ FTVtmp.substring(
															FTVtmp.lastIndexOf(", ") + 2,
															FTVtmp.length())));
						} catch (IOException e) {
							e.printStackTrace();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			}
			break;
		case 3:// <
			for (int i = 0; i < FactTableKey.size(); i++) {
				for (int j = 0; j < DimensionTableKey.size(); j++) {
					if (FactTableKey.get(i) < DimensionTableKey.get(j)) {
						try {
							FTVtmp = FactTableValue.get(i);
							// System.out.println("FactTableValue: "+FTVtmp);
							context.write(
									new Text(FTVtmp.substring(0,
											FTVtmp.lastIndexOf(", "))),
									new Text(
											(type / 6)
													+ "\t"
													+ DimensionTableValue
															.get(j)// Rdi
													+ FTVtmp.substring(
															FTVtmp.lastIndexOf(", ") + 2,
															FTVtmp.length())));
						} catch (IOException e) {
							e.printStackTrace();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			}
			break;
		case 4:// !=
			for (int i = 0; i < FactTableKey.size(); i++) {
				for (int j = 0; j < DimensionTableKey.size(); j++) {
					if (FactTableKey.get(i) != DimensionTableKey.get(j)) {
						try {
							FTVtmp = FactTableValue.get(i);
							// System.out.println("FactTableValue: "+FTVtmp);
							context.write(
									new Text(FTVtmp.substring(0,
											FTVtmp.lastIndexOf(", "))),
									new Text(
											(type / 6)
													+ "\t"
													+ DimensionTableValue
															.get(j)// Rdi
													+ FTVtmp.substring(
															FTVtmp.lastIndexOf(", ") + 2,
															FTVtmp.length())));
						} catch (IOException e) {
							e.printStackTrace();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			}
			break;
		}
	}

	@Override
	public void reduce(QuadTextPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		type = Integer.parseInt(key.getJoinCondition().toString());
		String tmp;
		switch (type % 6) {
		// TODO Theta Join
		case 0:// >=
		case 1:// <=
		case 2:// >
		case 3:// <
		case 4:// !=

			for (Text v : values) {
				tmp = v.toString();
				if (String.valueOf(tmp.charAt(0)).equals("D")) {
					DimensionTableKey.add(Integer.parseInt(key.getKey()
							.toString()));
					String DTVtmp = tmp.substring(3, tmp.length());
					if (DTVtmp.equals("null")) {
						DimensionTableValue.add("");
					} else {
						DimensionTableValue.add(DTVtmp);
					}
				} else if (String.valueOf(tmp.charAt(0)).equals("F")) {
					FactTableKey.add(Integer.parseInt(key.getKey().toString()));
					FactTableValue.add(tmp.substring(3, tmp.length()));
				}
			}
			break;
		case 5:// =
			List<String> fk = new ArrayList<String>();
			List<String> pk = new ArrayList<String>();

			for (Text v : values) {
				tmp = v.toString();

				// FIXLATER 只有支援一個表格一個顯示欄位
				if (tmp.contains(",")) {
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
				tmpArray = k.split(", ");
				// without rf
				for (int i = 0; i < tmpArray.length - 1; i++) {
					key_sb.append(tmpArray[i]);
					key_sb.append(", ");
				}
				// delete ", "
				if (key_sb.length() > 0) {
					key_sb.delete(key_sb.length() - 2, key_sb.length());
				}

				// column set start
				// FIXLATER 目前根據欄位給予編號(第三階段會使用)
				val_sb.append(key.getIndex());
				val_sb.append("\t");
				// column set end

				String[] tmptmp;
				for (String v : pk) {
					tmptmp = v.split(", ");
					for (int j = 0; j < tmptmp.length; j++) {
						if (!tmptmp[j].equals("")) {
							val_sb.append(tmptmp[j]);
							val_sb.append("\t");
						}
					}
				}
				val_sb.append(tmpArray[tmpArray.length - 1]);// rf
				context.write(new Text(key_sb.toString()),
						new Text(val_sb.toString()));
				val_sb.delete(0, val_sb.length());
				key_sb.delete(0, key_sb.length());
			}
		}

	}
}