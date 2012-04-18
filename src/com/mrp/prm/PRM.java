package com.mrp.prm;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mrp.lib.CheckAndDelete;
import com.mrp.lib.SQLParser;
import com.mrp.object.DefaultMain;
import com.mrp.object.QuadTextPair;
import com.mrp.tjsgm.FirstPhaseMapper;
import com.mrp.tjsgm.FirstPhaseReducer;
import com.mrp.tjsgm.SecondPhaseMapper;
import com.mrp.tjsgm.SecondPhaseReducer;
import com.mrp.tjsgm.TJSGMKeyPartitioner;

public class PRM extends DefaultMain {
	private final String FP_OUTPUT = "part-r-00000";

	@Override
	public boolean run(String query) {
		FUNCTION_NAME = "PRM";
		query = query.toUpperCase();
		boolean state = true;

		SQLParser parser = new SQLParser();
		state &= parser.parse(query + ".txt"); // file name
		wrieteGlobalInfoToHDFS(parser);

		if (state) {

			if (state) { // init conf
				Configuration conf = new Configuration();
				state &= doFirstPhase(query, conf, PATH_OUTPUT_FIRST, parser.getDimensionTables());
			}
			if (state) { // init conf
				Configuration conf = new Configuration();
				state &= doSecondPhase(query, conf, PATH_OUTPUT_SECOND, parser.getDimensionTables(), parser.getFilterTables(),
						parser.getTables().length - 1);
			}
			if (state) {
				Configuration conf = new Configuration();
				state &= doThirdPhase(query.toUpperCase(), conf, PATH_OUTPUT_THIRD);
			}
			if (state) {
				Configuration conf = new Configuration();
				state &= doForthPhase(query.toUpperCase(), conf, PATH_OUTPUT_FINAL);
			}
		}
		return state;
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
			job.setJarByClass(PRM.class);
			job.setMapperClass(FirstPhaseMapper.class);
			job.setReducerClass(FirstPhaseReducer.class);
			job.setOutputKeyClass(QuadTextPair.class);
			job.setOutputValueClass(Text.class);

			// all tables
			for (String t : table) {
				FileInputFormat.addInputPath(job, new Path(PATH_INPUT + SYSTEM_SPLIT + t + SYSTEM_SPLIT));
			}
			CheckAndDelete.checkAndDelete(outputPath, conf);
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			return job.waitForCompletion(true);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	protected boolean doSecondPhase(String query, Configuration conf, String outputPath, String[] dimensionTables,
			String[] filter_table, int numberOfReducer) {
		try {
			List<String> AllDimensionTalbes = new ArrayList<String>();
			AllDimensionTalbes.add(TABLE_CUSTOMER);
			AllDimensionTalbes.add(TABLE_DATE);
			AllDimensionTalbes.add(TABLE_PART);
			AllDimensionTalbes.add(TABLE_SUPPLIER);		
			DistributedCache.addCacheFile(new URI(FULL_PATH_COLUMN), conf);
			DistributedCache.addCacheFile(new URI(FULL_PATH_JOIN), conf);
			for (String t : dimensionTables) {
				DistributedCache.addCacheFile(new URI(PATH_BLOOM_FILTER + SYSTEM_SPLIT + AllDimensionTalbes.indexOf(t)),
						conf);
			}
			DistributedCache.addCacheFile(new URI(PATH_OUTPUT_FIRST + SYSTEM_SPLIT + FP_OUTPUT), conf);
			conf.setLong(MAPRED_TASK_TIMEOUT, Long.MAX_VALUE);
			Job job = new Job(conf, FUNCTION_NAME + " Second Phase " + query);
			job.setJarByClass(PRM.class);
			job.setMapperClass(PartitionAndReplicationPhaseMapper.class);
			job.setNumReduceTasks(numberOfReducer);
			job.setReducerClass(PartitionAndReplicationPhaseReducer.class);
			job.setOutputKeyClass(QuadTextPair.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(PATH_INPUT + SYSTEM_SPLIT + TABLE_LINEORDER + SYSTEM_SPLIT));// fact
			CheckAndDelete.checkAndDelete(outputPath, conf);
			FileOutputFormat.setOutputPath(job, new Path(outputPath));

			return job.waitForCompletion(true);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return false;
	}

}
