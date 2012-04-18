package com.mrp.tjsgm;

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

import com.mrp.object.BloomFilter;
import com.mrp.object.DefaultReducer;
import com.mrp.object.QuadTextPair;

public class FirstPhaseReducer extends DefaultReducer<QuadTextPair, QuadTextPair> {

	private static List<String> DIMENSION_TABLE;
	private BloomFilter<IntWritable>[] bloomfilter;
	private List<String> table;
	private FSDataOutputStream[] out;
	private final List<Integer> index = new ArrayList<Integer>();
	private final String PATH_BLOOM_FILTER = "MRP/bloomfilter/";

	@Override
	public void setup(Context context) throws IOException, InterruptedException {

		// init dimension table info
		DIMENSION_TABLE = new ArrayList<String>();
		DIMENSION_TABLE.add("customer");
		DIMENSION_TABLE.add("date");
		DIMENSION_TABLE.add("part");
		DIMENSION_TABLE.add("supplier");

		Configuration conf = new Configuration();
		FileSystem[] fs;
		// read distributed catch file
		Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		table = readLocalFile(localFiles[2]);

		bloomfilter = new BloomFilter[table.size()];
		fs = new FileSystem[table.size()];
		out = new FSDataOutputStream[table.size()];

		// create bloomfilter
		for (int i = 0; i < table.size(); i++) {
			bloomfilter[i] = new BloomFilter<IntWritable>();
			fs[i] = FileSystem.get(conf);
			Path outFile = new Path(PATH_BLOOM_FILTER + DIMENSION_TABLE.indexOf(table.get(i)));
			index.add(DIMENSION_TABLE.indexOf(table.get(i)));
			out[i] = fs[i].create(outFile, true);
		}

	}

	@Override
	public void reduce(QuadTextPair key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		bloomfilter[index.indexOf(Integer.parseInt(key.getIndex().toString()))].add(key.getKey());
		for (Text t : values) {
			context.write(key, t);
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (int i = 0; i < table.size(); i++) {
			bloomfilter[i].write(out[i]);
			out[i].close();
		}
	}
}