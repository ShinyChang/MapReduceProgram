package com.mrp.parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Shiny
 *         This is parser only work fine with star-join(Star Schema Benchmark)
 */
public class SQLParser {
	private final String AND = " and ";
	private final String BETWEEN = " between ";
	private String[] columns;
	private String content = "";
	private String[] dimensionTables;
	private BufferedReader bufferedReader = null;
	private final String[] FACT_TABLE = { "lineorder" };

	private String[] filters;
	private String[] filterTables;
	private final String FROM = " from ";

	private final String GROUP_BY = " group by ";
	private String[] groupby;
	private String[] joins;
	private final String[] OP = { ">=", "<=", ">", "<", "!=", "=" };
	private final String ORDER_BY = " order by ";
	private String[] orderby;
	private final String SELECT = "select ";
	private final String SPLIT_STRING = ",";

	private String[] tables;
	private final String UNDER_LINE = "_";

	private final String VAR_REGULAR = "^[a-z]\\w*";

	private final String WHERE = " where ";
	private final String WHITE_SPACE = " ";

	private String[] converStringToArray(String str, String split) {
		String[] tmp = str.split(split);
		String[] stringArray = new String[tmp.length];
		for (int i = 0; i < stringArray.length; i++) {
			stringArray[i] = tmp[i].trim();
		}
		return stringArray;
	}

	private String[] convertObjectArrayToStringArray(Object[] objectArray) {
		try {
			String[] intArray = new String[objectArray.length];
			for (int i = 0; i < objectArray.length; i++) {
				intArray[i] = (String) objectArray[i];
			}
			return intArray;
		} catch (Exception e) {
			return null;
		}
	}

	public String[] getColumns() {
		return columns;
	}

	public String getContent() {
		return content;
	}

	public String[] getDimensionTables() {
		return dimensionTables;
	}

	public String[] getFactTable() {
		return FACT_TABLE;
	}

	public String[] getFilters() {
		return filters;
	}

	public String[] getFilterTables() {
		return filterTables;
	}

	public String[] getGroupby() {
		return groupby;
	}

	public String[] getJoins() {
		return joins;
	}

	public String[] getOrderby() {
		return orderby;
	}

	public String[] getTables() {
		return tables;
	}

	public boolean isThetaJoin() {
		for (String join : joins) {
			for (int i = 0, j = OP.length - 1; i < j; i++) {// without equal op
				if (join.indexOf(OP[i]) > 0) {
					return true;
				}
			}
		}
		return false;
	}

	private boolean isVariable(String text) {
		return text.matches(VAR_REGULAR);
	}

	public boolean parse(String fileName) {
		return readFile(new File(fileName));
	}

	private String[] parseColumns() {
		int startIndex = content.indexOf(SELECT) + SELECT.length();
		int endIndex = content.indexOf(FROM);
		return converStringToArray(content.substring(startIndex, endIndex).trim(), SPLIT_STRING);
	}

	private String[] parseDimensionTables() {
		List<String> dimensionTables = new ArrayList<String>();
		for (String dt : tables) {
			if (!dt.equals(FACT_TABLE[0])) {
				dimensionTables.add(dt);
			}
		}
		return convertObjectArrayToStringArray(dimensionTables.toArray());
	}

	// this method should be runned after parseJoins
	private String[] parseFilters() {
		int startIndex = content.indexOf(WHERE) + WHERE.length();
		int endIndex = content.indexOf(GROUP_BY);
		String filters = content.substring(startIndex, endIndex).trim();
		for (String join : joins) {
			join = join.trim();
			filters = filters.replace(join, "");
			filters = filters.replaceFirst(AND, "");
		}
		filters = filters.trim();
		filters = filters.replaceAll(AND, SPLIT_STRING);

		// check if between
		String[] tmp = filters.split(BETWEEN);
		String filter_rule = "";
		for (int i = 0; i < tmp.length; i++) {
			// the string which is before the BETWEEN
			if (i % 2 == 0) {
				filter_rule += tmp[i] + WHITE_SPACE;
				// the string which is after the BETWEEN
			} else {
				filters = filters.trim();
				filter_rule += BETWEEN;
				filter_rule += tmp[i].replaceFirst(SPLIT_STRING, AND).trim();
				filters = filters.trim();
			}
		}
		return converStringToArray(filter_rule, SPLIT_STRING);
	}

	private String[] parseGroupBy() {
		int startIndex = content.indexOf(GROUP_BY) + GROUP_BY.length();
		int endIndex = content.indexOf(ORDER_BY);
		return converStringToArray(content.substring(startIndex, endIndex).trim(), SPLIT_STRING);
	}

	private String[] parseJoins() {
		int startIndex = content.indexOf(WHERE) + WHERE.length();
		int endIndex = content.indexOf(GROUP_BY);
		String[] filters = content.substring(startIndex, endIndex).trim().split(AND);
		String join = "";
		for (String filter : filters) {
			filter = filter.trim();
			for (String op : OP) {
				if (filter.indexOf(op) > 0) {
					String[] vars = filter.split(op);
					boolean isJoin = true;
					for (String v : vars) {
						v = v.trim();
						isJoin &= isVariable(v);
					}
					if (isJoin) {
						join += filter + SPLIT_STRING + WHITE_SPACE;
					}
				}
			}
		}
		return converStringToArray(join.substring(0, join.length() - 2), SPLIT_STRING);
	}

	private String[] parseOrderBy() {
		int startIndex = content.indexOf(ORDER_BY) + ORDER_BY.length();
		return converStringToArray(content.substring(startIndex).trim().replaceAll(";", ""), SPLIT_STRING);
	}

	private String[] parserFilterTables() {
		List<String> temp = new ArrayList<String>();
		for (String filter : filters) {
			for (String tt : dimensionTables) {
				if (filter.contains(tt.substring(0, 1) + UNDER_LINE)) {// tableTitle_columnName
					if (!temp.contains(tt)) {
						temp.add(tt);
					}
				}
			}
		}
		return convertObjectArrayToStringArray(temp.toArray());
	}

	private String[] parseTables() {
		int startIndex = content.indexOf(FROM) + FROM.length();
		int endIndex = content.indexOf(WHERE);
		return converStringToArray(content.substring(startIndex, endIndex).trim(), SPLIT_STRING);
	}

	private boolean readFile(File file) {
		try {
			bufferedReader = new BufferedReader(new FileReader(file));
			while (bufferedReader.ready()) {
				content += bufferedReader.readLine() + WHITE_SPACE;
			}
			bufferedReader.close();
			content = new String(content.getBytes("8859_1"), "BIG5");
			content.toLowerCase();
			columns = parseColumns();
			joins = parseJoins();
			filters = parseFilters();
			groupby = parseGroupBy();
			orderby = parseOrderBy();
			tables = parseTables();
			dimensionTables = parseDimensionTables();
			filterTables = parserFilterTables();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
}
