package com.mrp.shared;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;

import com.mrp.object.DefaultReducer;

public class ThirdPhaseReducer extends DefaultReducer<Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		int tableIndex;
		List<String> column = new ArrayList<String>();
		List<Integer> columnIdx = new ArrayList<Integer>();
		Map<Integer, Long> sumOfRfMap = new HashMap<Integer, Long>();
		String tmp;
		String[] tmpValue;

		for (Text v : values) {
			tmp = v.toString();
			tmpValue = tmp.split(TAB);
			tableIndex = Integer.parseInt(tmpValue[0]);

			// each rDi
			for (int i = 1; i < tmpValue.length - 1; i++) {
				if (!columnIdx.contains(tableIndex)) {

					// 變動大小LIST 避免無法加入
					if (column.size() < tableIndex + 1) {
						int size = column.size();
						for (int j = 0; j <= tableIndex - size + 1; j++) {
							column.add(TAB);
						}
					}
					column.set(tableIndex, tmpValue[i]);
					columnIdx.add(tableIndex);
				}
			}

			if (!sumOfRfMap.containsKey(tableIndex)) {
				sumOfRfMap.put(tableIndex, Long.parseLong(tmpValue[tmpValue.length - 1]));
			} else {
				sumOfRfMap.put(tableIndex, sumOfRfMap.get(tableIndex) + Long.parseLong(tmpValue[tmpValue.length - 1]));
			}

		}

		// 排序
		Collections.sort(columnIdx);

		// 建立KEY，VALUE
		StringBuffer sb = new StringBuffer();
		for (int idx : columnIdx) {
			sb.append(column.get(idx));
			sb.append(TAB);
		}

		// equi-join
		boolean isEquiJoin = true;

		// get first key
		long temp = sumOfRfMap.get(sumOfRfMap.keySet().toArray()[0]);
		long MAX_RF = Long.MIN_VALUE;
		for (int k : sumOfRfMap.keySet()) {
			isEquiJoin &= temp == sumOfRfMap.get(k);
		}
		if (isEquiJoin) {
			context.write(new Text(sb.toString()), new Text(String.valueOf(temp)));
		} else {
			// FIXLATER 只有支援一個Theta-join
			for (int k : sumOfRfMap.keySet()) {
				if (MAX_RF < sumOfRfMap.get(k)) {
					MAX_RF = sumOfRfMap.get(k);
				}
			}
			context.write(new Text(sb.toString()), new Text(String.valueOf(MAX_RF)));
		}

	}
}
