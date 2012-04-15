package com.mrp.lib;

import java.util.Arrays;
import java.util.List;

public class ConditionValidator {
	private final String[][] SCHEMA = {
			{ "c_custkey", "c_name", "c_address", "c_city", "c_nation", "c_region", "c_phone", "c_mktsegment" },
			{ "d_datekey", "d_date", "d_dayofweek", "d_month", "d_year", "d_yearmonthnum", "d_yearmonth", "d_daynuminweek", "d_daynuminmonth",
					"d_daynuminyear", "d_monthnuminyear", "d_weeknuminyear", "d_sellingseason", "d_lastdayinmonthfl", "d_holidayfl", "d_weekdayfl",
					"d_daynuminmonth" }, { "p_partkey", "p_name", "p_mfgr", "p_category", "p_brand1", "p_color", "p_type", "p_size", "p_container" },
			{ "s_suppkey", "s_name", "s_address", "s_city", "s_nation", "s_region", "s_phone" } };

	private final String[] OP = { ">=", "<=", ">", "<", "!=", "=" };
	private final String BETWEEN = " between ";
	private final String OR = " or ";
	private final String AND = " and ";
	private final String APOSTROPHE = "'";
	private List<String> filters;

	public boolean valid(int tableIndex, int columnIndex, String value) {
		boolean result = true;
		boolean findColumn = false;
		for (String filter : filters) {

			// 尋找該欄位的篩選條件
			if (filter.contains(SCHEMA[tableIndex][columnIndex])) {
				findColumn = true;
				if (filter.contains(BETWEEN)) {
					result &= validBetween(filter, value);
				} else if (filter.contains(OR)) {
					result &= validOr(filter, value);
				} else {
					result &= validNormal(filter, value);
				}
			}
		}
		
		return result && findColumn;
	}

	private boolean validBetween(String filter, String value) {
		String[] temp = filter.split(AND);
		String MAX = temp[1].trim();
		if (MAX.contains(APOSTROPHE)) {
			MAX = MAX.substring(1, MAX.length() - 1);
		}
		temp = temp[0].split(BETWEEN);
		String MIN = temp[1].trim();
		if (MIN.contains(APOSTROPHE)) {
			MIN = MIN.substring(1, MIN.length() - 1);
		}
		if (value.compareTo(MAX) <= 0 && value.compareTo(MIN) >= 0) {
			return true;
		}
		return false;
	}

	private boolean validOr(String filter, String value) {
		filter = filter.trim();
		filter = filter.substring(1, filter.length() - 1);//clear ( )
		String[] temp = filter.split(OR);
		int require = temp.length;
		int op[] = new int[temp.length];
		String filterValue[] = new String[temp.length];
		int cnt = 0;
		for (String rule : temp) {
			rule = rule.trim();
			for (int i = 0; i < OP.length; i++) {
				if (rule.contains(OP[i])) {
					op[cnt] = i;
					String tmp = rule.split(OP[i])[1].trim();
					if (tmp.contains(APOSTROPHE)) {
						tmp = tmp.substring(1, tmp.length() - 1);
					}
					filterValue[cnt] = tmp;
					cnt++;
					break;
				}

			}
		}
		cnt = 0;
		int result = -2;
		for (int i = 0; i < require; i++) {
			result = value.compareTo(filterValue[i]);
			if (finalCheck(result, op[i])) {
				cnt++;
			}
		}
		return cnt > 0;
	}

	private boolean validNormal(String filter, String value) {
		String filterValue;
		int result = -2;
		int i;
		for (i = 0; i < OP.length; i++) {
			if (filter.contains(OP[i])) {
				filterValue = filter.split(OP[i])[1].trim();
				if (filterValue.contains(APOSTROPHE)) {
					filterValue = filterValue.substring(1, filterValue.length() - 1);
				}
				result = value.compareToIgnoreCase(filterValue);
				break;
			}
		}
		return finalCheck(result, i);
	}

	private boolean finalCheck(int result, int opIndex) {
		if (result > 0) {
			if (opIndex % 2 == 0) {
				return true;
			}
		} else if (result < 0) {
			if (opIndex == 1 || opIndex == 3 || opIndex == 4) {
				return true;
			}
		} else if (result == 0) {
			if (opIndex == 0 || opIndex == 2 || opIndex == 5) {
				return true;
			}
		}
		return false;
	}

	public void loadFilter(List<String> filters) {
		this.filters = filters;
	}

	public void loadFilter(String[] filters) {
		this.filters = Arrays.asList(filters);
	}
}
