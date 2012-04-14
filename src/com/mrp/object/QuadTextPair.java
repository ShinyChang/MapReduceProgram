package com.mrp.object;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class QuadTextPair implements WritableComparable<QuadTextPair> {
	private IntWritable key;
	private IntWritable index;
	private IntWritable joinCondition;
	private Text tableType;

	public QuadTextPair() {
		set(new IntWritable(), new IntWritable(), new IntWritable(), new Text());
	}

	public QuadTextPair(String first, String second, String joinCondition,
			String tableType) {
		set(new IntWritable(Integer.parseInt(first)),
				new IntWritable(Integer.parseInt(second)), new IntWritable(
						Integer.parseInt(joinCondition)), new Text(tableType));
	}

	public QuadTextPair(IntWritable key, IntWritable index,
			IntWritable joinCondition, Text tableType) {
		set(key, index, joinCondition, tableType);
	}

	public void set(IntWritable key, IntWritable index,
			IntWritable joinCondition, Text tableType) {
		this.key = key;
		this.index = index;
		this.joinCondition = joinCondition;
		this.tableType = tableType;
	}

	public IntWritable getKey() {
		return key;
	}

	public IntWritable getIndex() {
		return index;
	}

	public IntWritable getJoinCondition() {
		return joinCondition;
	}

	public Text getTableType() {
		return tableType;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		key.readFields(in);
		index.readFields(in);
		joinCondition.readFields(in);
		tableType.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		key.write(out);
		index.write(out);
		joinCondition.write(out);
		tableType.write(out);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((index == null) ? 0 : index.hashCode());
		result = prime * result
				+ ((joinCondition == null) ? 0 : joinCondition.hashCode());
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		//略過TableType的判斷
		/*
		result = prime * result
				+ ((tableType == null) ? 0 : tableType.hashCode());*/
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		QuadTextPair other = (QuadTextPair) obj;
		if (index == null) {
			if (other.index != null)
				return false;
		} else if (!index.equals(other.index))
			return false;
		if (joinCondition == null) {
			if (other.joinCondition != null)
				return false;
		} else if (!joinCondition.equals(other.joinCondition))
			return false;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		//略過TableType的判斷
		/*
		if (tableType == null) {
			if (other.tableType != null)
				return false;
		} else if (!tableType.equals(other.tableType))
			return false;
			*/
		return true;
	}

	@Override
	public String toString() {
		return key.toString() + "\t" + index.toString() + "\t"
				+ joinCondition.toString() + "\t" + tableType.toString();
	}

	@Override
	public int compareTo(QuadTextPair tp) {
		int cmp = key.compareTo(tp.key);
		if (cmp != 0) {
			return cmp;
		}
		cmp = index.compareTo(tp.index);
		if (cmp != 0) {
			return cmp;
		}
		return joinCondition.compareTo(tp.joinCondition);
	}


}