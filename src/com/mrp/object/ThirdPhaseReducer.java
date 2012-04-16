package com.mrp.object;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ThirdPhaseReducer extends Reducer<Text, Text, Text, Text> {
	int CNT = 0;

	// initial, only do once
	public void setup(Context context) {
		try {
			DistributedCache.getLocalCacheFiles(context.getConfiguration());
			Path[] localFiles = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			FileReader fr = new FileReader(localFiles[0].toString());
			BufferedReader br = new BufferedReader(fr);
			while (br.ready()) {
				br.readLine();// do nothing
				CNT++;
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		int tableIndex;
		int cnt = 0;
		List<String> column = new ArrayList<String>();
		List<Integer> columnIdx = new ArrayList<Integer>();

		List<String> RF = new ArrayList<String>();
		String tmp;
		String[] tmpValue;
		for (Text v : values) {
			tmp = v.toString();
			tmpValue = tmp.split("\t");// tableIdx, rdi, RF
			tableIndex = Integer.parseInt(tmpValue[0]);
			
			//each rDi
			for (int i = 1; i < tmpValue.length - 1; i++) {
				if (!columnIdx.contains(tableIndex)) {
					
					//變動大小LIST 避免無法加入
					if (column.size() < tableIndex + 1) {
						int size = column.size();
						for (int j = 0; j <= tableIndex - size + 1; j++) {
							column.add("\t");
						}
					}
					column.set(tableIndex, tmpValue[i]);
					columnIdx.add(tableIndex);
				}
			}

			//THETA JOIN
			if (++cnt >= CNT) {
				RF.add(tmpValue[tmpValue.length - 1]);
			}

		}
		
		//排序
		Collections.sort(columnIdx);
		
		//建立KEY，VALUE
		StringBuffer sb = new StringBuffer();
		for (int idx : columnIdx) {
			sb.append(column.get(idx));
			sb.append("\t");
		}
		for (String rf : RF) {
			context.write(new Text(sb.toString()), new Text(rf));
		}

	}
}
