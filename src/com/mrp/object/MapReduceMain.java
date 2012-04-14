package com.mrp.object;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

public class MapReduceMain {
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

	public boolean run(String query) {
		return false;
	}

	protected boolean doFirstPhase(String query, Configuration conf,
			String outputPath, String[] table) {
		return false;
	}

	protected boolean doFirstPhase(String query, Configuration conf,
			String outputPath, List<String> table) {
		return false;
	}

	protected boolean doSecondPhase(String query, Configuration conf,
			String outputPath, String[] table, List<String> filter_table,
			int numberOfReducer) {
		return false;
	}

	protected boolean doThirdPhase(String query, Configuration conf,
			String outputPath) {
		return false;
	}

	protected boolean doForthPhase(String query, Configuration conf,
			String outputPath)  {
		return false;
	}
}
