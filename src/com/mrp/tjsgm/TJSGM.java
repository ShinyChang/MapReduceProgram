package com.mrp.tjsgm;

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
import com.mrp.lib.WriteHDFS;
import com.mrp.object.FinalPhaseMapper;
import com.mrp.object.FinalPhaseReducer;
import com.mrp.object.MapReduceMain;
import com.mrp.object.ThirdPhaseMapper;
import com.mrp.object.ThirdPhaseReducer;
import com.mrp.object.QuadTextPair;
import com.mrp.parser.SQLParser;

public class TJSGM extends MapReduceMain {
	private final String FUNCTION_NAME = "TJSGM";

	protected void wrieteGlobalInfoToHDFS(SQLParser parser) {
		WriteHDFS writeHDFS = new WriteHDFS();
		writeHDFS.writeGlobalInfo(GLOBAL_INFO_COLUMN, parser.getColumns());
		writeHDFS.writeGlobalInfo(GLOBAL_INFO_FILTER, parser.getFilters());
		writeHDFS.writeGlobalInfo(GLOBAL_INFO_FILTER_TABLE,
				parser.getFilterTables());
		writeHDFS.writeGlobalInfo(GLOBAL_INFO_JOIN, parser.getJoins());
		writeHDFS.writeGlobalInfo(GLOBAL_INFO_TABLE, parser.getTables());
		writeHDFS.writeGlobalInfo(GLOBAL_INFO_DIMENSION_TABLE,
				parser.getDimensionTables());
		writeHDFS
				.writeGlobalInfo(GLOBAL_INFO_FACT_TABLE, parser.getFactTable());
		writeHDFS.writeGlobalInfo(GLOBAL_INFO_GROUP_BY, parser.getGroupby());
		writeHDFS.writeGlobalInfo(GLOBAL_INFO_ORDER_BY, parser.getOrderby());
	}

	@Override
	public boolean run(String query) {
		query = query.toUpperCase();
		boolean state = true;
		boolean isThetaJoin;

		SQLParser parser = new SQLParser();
		state &= parser.parse(query + ".txt"); // file name
		wrieteGlobalInfoToHDFS(parser);

		if (state) {

			// TODO Theta Join
			isThetaJoin = parser.isThetaJoin();

			if (state) { // init conf
				Configuration conf = new Configuration();
				state &= doFirstPhase(query, conf, PATH_OUTPUT_FIRST,
						parser.getFilterTables());
			}
			if (state) { // init conf
				Configuration conf = new Configuration();
				state &= doSecondPhase(query, conf, PATH_OUTPUT_SECOND,
						parser.getTables(), parser.getFilterTables(),
						parser.getTables().length - 1);
			}
			if (state) {
				Configuration conf = new Configuration();
				state &= doThirdPhase(query.toUpperCase(), conf,
						PATH_OUTPUT_THIRD);
			}
			if (state) {
				Configuration conf = new Configuration();
				state &= doForthPhase(query.toUpperCase(), conf,
						PATH_OUTPUT_FINAL);
			}
		}
		return super.run(query);
	}

	@Override
	protected boolean doForthPhase(String query, Configuration conf,
			String outputPath) {
		try {
			conf.setLong(MAPRED_TASK_TIMEOUT, Long.MAX_VALUE);
			Job job = new Job(conf, FUNCTION_NAME + " Final Phase " + query);
			job.setJarByClass(TJSGM.class);
			job.setMapperClass(FinalPhaseMapper.class);
			job.setReducerClass(FinalPhaseReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(PATH_OUTPUT_THIRD));// merge_output
			CheckAndDelete.checkAndDelete(outputPath, conf);
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			return job.waitForCompletion(true);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	protected boolean doThirdPhase(String query, Configuration conf,
			String outputPath) {
		try {
			DistributedCache.addCacheFile(new URI(FULL_PATH_JOIN), conf);
			conf.setLong(MAPRED_TASK_TIMEOUT, Long.MAX_VALUE);
			Job job = new Job(conf, FUNCTION_NAME + " Third Phase " + query);
			job.setJarByClass(TJSGM.class);
			job.setMapperClass(ThirdPhaseMapper.class);
			job.setReducerClass(ThirdPhaseReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(PATH_OUTPUT_SECOND));// SnG_output

			CheckAndDelete.checkAndDelete(outputPath, conf);
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			return job.waitForCompletion(true);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	protected boolean doFirstPhase(String query, Configuration conf,
			String outputPath, String[] table) {
		try {

			// global information
			DistributedCache.addCacheFile(new URI(FULL_PATH_COLUMN), conf);
			DistributedCache.addCacheFile(new URI(FULL_PATH_FILTER), conf);
			DistributedCache
					.addCacheFile(new URI(FULL_PATH_FILTER_TABLE), conf);
			DistributedCache.addCacheFile(new URI(FULL_PATH_JOIN), conf);

			// new job
			Job job = new Job(conf, FUNCTION_NAME + " First Phase " + query);
			job.setJarByClass(TJSGM.class);
			job.setMapperClass(FirstPhaseMapper.class);
			job.setReducerClass(FirstPhaseReducer.class);
			job.setOutputKeyClass(QuadTextPair.class);
			job.setOutputValueClass(Text.class);

			// filter table
			for (String t : table) {
				FileInputFormat.addInputPath(job, new Path(PATH_INPUT
						+ SYSTEM_SPLIT + t + SYSTEM_SPLIT));
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
	protected boolean doSecondPhase(String query, Configuration conf,
			String outputPath, String[] table, String[] filter_table,
			int numberOfReducer) {
		try {
			List<String> dimension_talbe = new ArrayList<String>();
			dimension_talbe.add(TABLE_CUSTOMER);
			dimension_talbe.add(TABLE_DATE);
			dimension_talbe.add(TABLE_PART);
			dimension_talbe.add(TABLE_SUPPLIER);

			DistributedCache.addCacheFile(new URI(FULL_PATH_COLUMN), conf);
			DistributedCache.addCacheFile(new URI(FULL_PATH_JOIN), conf);

			for (String t : filter_table) {
				DistributedCache.addCacheFile(new URI(PATH_BLOOM_FILTER
						+ SYSTEM_SPLIT + dimension_talbe.indexOf(t)), conf);
			}
			conf.setLong(MAPRED_TASK_TIMEOUT, Long.MAX_VALUE);
			Job job = new Job(conf, FUNCTION_NAME + " Second Phase " + query);
			job.setJarByClass(TJSGM.class);
			job.setMapperClass(SecondPhaseMapper.class);
			job.setPartitionerClass(TJSGMKeyPartitioner.class);
			job.setNumReduceTasks(numberOfReducer);
			job.setReducerClass(SecondPhaseReducer.class);
			job.setOutputKeyClass(QuadTextPair.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(PATH_INPUT
					+ SYSTEM_SPLIT + TABLE_LINEORDER + SYSTEM_SPLIT));// fact
			FileInputFormat.addInputPath(job, new Path(PATH_OUTPUT_FIRST));// 第一階段結果
			List<String> list = new ArrayList<String>();
			for (String s : table) {// all need table
				if (!s.equals(TABLE_LINEORDER)) {
					list.add(s);
				}
			}
			for (String s : filter_table) {// filtered table
				list.remove(s);
			}
			for (String t : list) {// non-filter table
				FileInputFormat.addInputPath(job, new Path(PATH_INPUT
						+ SYSTEM_SPLIT + t + SYSTEM_SPLIT));
			}
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
