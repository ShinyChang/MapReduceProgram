package com.mrp.object;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class DefaultMapper<T> extends Mapper<Object, Text, T, Text> {
	protected T outputKey;
	protected Text outputValue = new Text();
	protected int tableIndex = -1;
	protected int joinIndex = -1;
	protected String fileName;

	protected final String VERTICALBAR = "|";
	protected final String TAB = "\t";
	protected final String MINUS = "-";
	protected final String RIGHT_BRACKET = ")";
	protected final String LEFT_BRACKET = "(";
	protected final String SUM = "sum";
	protected final String UNDER_LINE = "_";
	protected final String DIMENSION_TABLE_SIGN = "D";
	protected final String FACT_TABLE_SIGN = "F";
	protected final String COMMA = ",";
	protected final String WHITE_SPACE = " ";

	protected final String[] DIMENSION_TABLE_INDEX = { "customer.tbl", "date.tbl", "part.tbl", "supplier.tbl" };
	protected final String[][] SCHEMA = {
			{ "c_custkey", "c_name", "c_address", "c_city", "c_nation", "c_region", "c_phone", "c_mktsegment" },
			{ "d_datekey", "d_date", "d_dayofweek", "d_month", "d_year", "d_yearmonthnum", "d_yearmonth",
					"d_daynuminweek", "d_daynuminmonth", "d_daynuminyear", "d_monthnuminyear", "d_weeknuminyear",
					"d_sellingseason", "d_lastdayinmonthfl", "d_holidayfl", "d_weekdayfl", "d_daynuminmonth" },
			{ "p_partkey", "p_name", "p_mfgr", "p_category", "p_brand1", "p_color", "p_type", "p_size", "p_container" },
			{ "s_suppkey", "s_name", "s_address", "s_city", "s_nation", "s_region", "s_phone" } };

	protected final String[] FACT_TABLE_SCHEMA = { "lo_orderkey", "lo_linenumber", "lo_custkey", "lo_partkey",
			"lo_suppkey", "lo_orderdate", "lo_ordpriority", "lo_shippriority", "lo_quantity", "lo_extendedprice",
			"lo_ordtotalprice", "lo_discount", "lo_revenue", "lo_supplycost", "lo_tax", "lo_commitdate", "lo_shipmode" };
	protected final int[] FACT_TABLE_FOREIGN_INDEX = { -1, -1, 0, 2, 3, 1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 1, -1 };
	protected final String[] OP = { ">=", "<=", ">", "<", "!=", "=" };

	protected void readTableIndex(Context context) {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		for (int i = 0; i < DIMENSION_TABLE_INDEX.length; i++) {
			if (fileName.equals(DIMENSION_TABLE_INDEX[i])) {
				tableIndex = i;
				break;
			}
		}
	}

	protected void readJoinIndex(Context context, List<String> join) {
		// 如果來自Fact Table直接離開
		if (tableIndex < 0) {
			return;
		}
		String first_word = DIMENSION_TABLE_INDEX[tableIndex].substring(0, 1);
		for (int i = 0; i < join.size(); i++) {
			for (int j = 0; j < OP.length; j++) {
				if (join.get(i).indexOf(OP[j]) > 0) {
					if (first_word.equals(String.valueOf(join.get(i).split(OP[j])[1].trim().charAt(0)))) {
						joinIndex = i * 6 + j;
					}
					break;
				}
			}
		}
	}

	protected List<String> readLocalFile(Path localFiles) throws IOException {
		List<String> tmpList = new ArrayList<String>();
		FileReader fr = new FileReader(localFiles.toString());
		BufferedReader br = new BufferedReader(fr);
		while (br.ready()) {
			tmpList.add(br.readLine());
		}
		br.close();
		return tmpList;
	}

	protected String[] readRow(Text row, String splitSign) {
		StringTokenizer itr = new StringTokenizer(row.toString(), splitSign);
		int tableLength = itr.countTokens();
		String[] tmpToken = new String[tableLength];
		for (int i = 0; i < tableLength; i++) {
			tmpToken[i] = itr.nextToken();
		}
		return tmpToken;
	}
}
