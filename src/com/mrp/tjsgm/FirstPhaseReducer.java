package com.mrp.tjsgm;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.mrp.object.BloomFilter;
import com.mrp.object.QuadTextPair;

public class FirstPhaseReducer extends Reducer<QuadTextPair, Text, QuadTextPair, Text> {

	private static List<String> DIMENSION_TABLE;
	private BloomFilter<IntWritable>[] bf;
	private List<String> table = new ArrayList<String>();
	private FSDataOutputStream[] out;
	private List<Integer> index = new ArrayList<Integer>();

	// initial, only do once
	public void setup(Context context) {

		// init dimension table info
		DIMENSION_TABLE = new ArrayList<String>();
		DIMENSION_TABLE.add("customer");
		DIMENSION_TABLE.add("date");
		DIMENSION_TABLE.add("part");
		DIMENSION_TABLE.add("supplier");

		Configuration conf = new Configuration();
		FileSystem[] fs;
		try {
			// read distributed catch file
			DistributedCache.getLocalCacheFiles(context.getConfiguration());
			Path[] localFiles = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());

			// table
			FileReader fr = new FileReader(localFiles[2].toString());
			BufferedReader br = new BufferedReader(fr);
			while (br.ready()) {
				table.add(br.readLine());
			}
			br.close();

			// init
			bf = new BloomFilter[table.size()];
			fs = new FileSystem[table.size()];
			out = new FSDataOutputStream[table.size()];

			// create bloomfilter
			for (int i = 0; i < table.size(); i++) {
				bf[i] = new BloomFilter<IntWritable>();
				fs[i] = FileSystem.get(conf);
				Path outFile = new Path("MRP/bloomfilter/"
						+ DIMENSION_TABLE.indexOf(table.get(i)));
				index.add(DIMENSION_TABLE.indexOf(table.get(i)));
				out[i] = fs[i].create(outFile, true);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void cleanup(Context context) {
		try {
			for (int i = 0; i < table.size(); i++) {
				bf[i].write(out[i]);
				out[i].close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void reduce(QuadTextPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		bf[index.indexOf(Integer.parseInt(key.getIndex().toString()))].add(key
				.getKey());
		for (Text t : values) {
			context.write(key, t);
		}
	}
}