package com.mrp.tjsgm;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mrp.lib.CheckAndDelete;
import com.mrp.lib.WriteHDFS;
import com.mrp.object.MapReduceMain;
import com.mrp.object.QuadTextPair;
import com.mrp.parser.SQLParser;

public class TJSGM extends MapReduceMain {
	private final String FUNCTION_NAME = "TJSGM";

	@Override
	public boolean run(String query) {
		query = query.toUpperCase();
		boolean state = true;
		boolean isThetaJoin;
		SQLParser parser = new SQLParser();
		WriteHDFS writeHDFS = new WriteHDFS();

		// parse query
		state &= parser.parse(query + ".txt"); // file name
		if (state) {
			isThetaJoin = parser.isThetaJoin();

			writeHDFS.writeGlobalInfo(GLOBAL_INFO_COLUMN, parser.getColumns());
			writeHDFS.writeGlobalInfo(GLOBAL_INFO_FILTER, parser.getFilters());
			writeHDFS.writeGlobalInfo(GLOBAL_INFO_FILTER_TABLE, parser.getFilterTables());
			writeHDFS.writeGlobalInfo(GLOBAL_INFO_JOIN, parser.getJoins());
			writeHDFS.writeGlobalInfo(GLOBAL_INFO_TABLE, parser.getTables());
			writeHDFS.writeGlobalInfo(GLOBAL_INFO_DIMENSION_TABLE, parser.getDimensionTables());
			writeHDFS.writeGlobalInfo(GLOBAL_INFO_FACT_TABLE, parser.getFactTable());
			writeHDFS.writeGlobalInfo(GLOBAL_INFO_GROUP_BY, parser.getGroupby());
			writeHDFS.writeGlobalInfo(GLOBAL_INFO_ORDER_BY, parser.getOrderby());

			if (state) { // init conf
				Configuration conf = new Configuration();
				state &= doFirstPhase(query, conf, PATH_OUTPUT_FIRST, parser.getFilterTables());
			}
		}
		return super.run(query);
	}

	@Override
	protected boolean doFirstPhase(String query, Configuration conf, String outputPath, String[] table) {
		try {

			// global information
			DistributedCache.addCacheFile(new URI(FULL_PATH_COLUMN), conf);
			DistributedCache.addCacheFile(new URI(FULL_PATH_FILTER), conf);
			DistributedCache.addCacheFile(new URI(FULL_PATH_FILTER_TABLE), conf);
			DistributedCache.addCacheFile(new URI(FULL_PATH_JOIN), conf);

			// new job
			Job job = new Job(conf, FUNCTION_NAME + " First Phase " + query);
			job.setJarByClass(TJSGM.class);
			job.setMapperClass(First_Phase_Mapper.class);
			job.setReducerClass(First_Phase_Reducer.class);
			job.setOutputKeyClass(QuadTextPair.class);
			job.setOutputValueClass(Text.class);

			// filter table
			for (String t : table) {
				FileInputFormat.addInputPath(job, new Path(PATH_INPUT + SYSTEM_SPLIT + t + SYSTEM_SPLIT));
			}
			CheckAndDelete.checkAndDelete(outputPath, conf);
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			return job.waitForCompletion(true);
		} catch (URISyntaxException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return false;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
	}

}