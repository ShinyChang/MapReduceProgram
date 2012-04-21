package com.mrp.object;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
import com.mrp.lib.WriteHDFS;
import com.mrp.shared.FinalPhaseMapper;
import com.mrp.shared.FinalPhaseReducer;
import com.mrp.shared.ThirdPhaseMapper;
import com.mrp.shared.ThirdPhaseReducer;
import com.mrp.tjsgm.TJSGM;

public class DefaultMain {
	protected String FUNCTION_NAME = "Default";
	protected final String SYSTEM_SPLIT = "/";
	
	// GLOBAL INFO
	protected final String GLOBAL_PATH = "global";
	protected final String GLOBAL_INFO_COLUMN = "column";
	protected final String GLOBAL_INFO_TABLE = "table";
	protected final String GLOBAL_INFO_RELATION = "relation";
	protected final String GLOBAL_INFO_FILTER = "filter";
	protected final String GLOBAL_INFO_FILTER_TABLE = "filter_table";
	protected final String GLOBAL_INFO_JOIN = "join";
	protected final String GLOBAL_INFO_DIMENSION_TABLE ="dimension_table";
	protected final String GLOBAL_INFO_FACT_TABLE ="fact_table";	
	protected final String GLOBAL_INFO_GROUP_BY = "group_by";
	protected final String GLOBAL_INFO_ORDER_BY = "order_by";
	
	// TEMP OUTPUT PATH
	protected final String PATH_OUTPUT_FIRST = "MRP/output_1st";
	protected final String PATH_OUTPUT_SECOND = "MRP/output_2nd";
	protected final String PATH_OUTPUT_THIRD = "MRP/output_3rd";
	protected final String PATH_OUTPUT_FINAL = "MRP/output_final";

	// PATH(Main, Input, Parameter)
	protected final String PATH = "MRP";
	protected final String PATH_INPUT = "MRP/input";
	protected final String PATH_BLOOM_FILTER = "MRP/bloomfilter";

	// GLOBAL INFO FULL PATH
	protected final String FULL_PATH_GLOBAL = "MRP/global";
	protected final String FULL_PATH_COLUMN = "MRP/global/column";
	protected final String FULL_PATH_JOIN = "MRP/global/join";
	protected final String FULL_PATH_RELATION = "MRP/global/relation";
	protected final String FULL_PATH_TABLE = "MRP/global/table";
	protected final String FULL_PATH_FILTER = "MRP/global/filter";
	protected final String FULL_PATH_FILTER_TABLE = "MRP/global/filter_table";
	protected final String FULL_PATH_DIMENSION_TABLE ="MRP/global/dimension_table";
	protected final String FULL_PATH_FACT_TABLE ="MRP/global/fact_table";
	protected final String FULL_PATH_GROUP_BY ="MRP/global/group_by";
	protected final String FULL_PATH_ORDER_BY ="MRP/global/order_by";

	// TABLE SCHEMA
	protected final String TABLE_CUSTOMER = "customer";
	protected final String TABLE_PART = "part";
	protected final String TABLE_LINEORDER = "lineorder";
	protected final String TABLE_SUPPLIER = "supplier";
	protected final String TABLE_DATE = "date";
	
	// MAPREDUCE SYSTEM PARAMETER
	protected final String MAPRED_TASK_TIMEOUT = "mapred.task.timeout";

	protected void wrieteGlobalInfoToHDFS(SQLParser parser) {
		WriteHDFS writeHDFS = new WriteHDFS();
		writeHDFS.writeGlobalInfo(GLOBAL_INFO_COLUMN, parser.getColumns());
		writeHDFS.writeGlobalInfo(GLOBAL_INFO_FILTER, parser.getFilters());
		writeHDFS.writeGlobalInfo(GLOBAL_INFO_FILTER_TABLE, parser.getFilterTables());
		writeHDFS.writeGlobalInfo(GLOBAL_INFO_JOIN, parser.getJoins());
		writeHDFS.writeGlobalInfo(GLOBAL_INFO_TABLE, parser.getTables());
		writeHDFS.writeGlobalInfo(GLOBAL_INFO_DIMENSION_TABLE, parser.getDimensionTables());
		writeHDFS.writeGlobalInfo(GLOBAL_INFO_FACT_TABLE, parser.getFactTable());
		writeHDFS.writeGlobalInfo(GLOBAL_INFO_GROUP_BY, parser.getGroupby());
		writeHDFS.writeGlobalInfo(GLOBAL_INFO_ORDER_BY, parser.getOrderby());
	}
	
	public long run(String query) {
		return -1L;
	}

	protected boolean doFirstPhase(String query, Configuration conf,
			String outputPath, String[] table) {
		return false;
	}

	protected boolean doThirdPhase(String query, Configuration conf, String outputPath) {
		try {
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
		} 
		return false;
	}

	protected boolean doForthPhase(String query, Configuration conf,
			String outputPath)  {
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

	protected boolean doSecondPhase(String query, Configuration conf,
			String outputPath, String[] table, String[] filter_table,
			int numberOfReducer) {
		return false;
	}
}
