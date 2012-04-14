package com.mrp.object;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class TripleTextPair implements WritableComparable<TripleTextPair> {
	private IntWritable key;
	private IntWritable index;
	private IntWritable type;

	public TripleTextPair() {
		set(new IntWritable(), new IntWritable(), new IntWritable());
	}

	public TripleTextPair(String first, String second, String type) {
		set(new IntWritable(Integer.parseInt(first)),
				new IntWritable(Integer.parseInt(second)), new IntWritable(
						Integer.parseInt(type)));
	}

	public TripleTextPair(IntWritable key, IntWritable index, IntWritable type) {
		set(key, index, type);
	}

	public void set(IntWritable key, IntWritable index, IntWritable type) {
		this.key = key;
		this.index = index;
		this.type = type;
	}

	public IntWritable getKey() {
		return key;
	}

	public IntWritable getIndex() {
		return index;
	}

	public IntWritable getType() {
		return type;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		key.readFields(in);
		index.readFields(in);
		type.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		key.write(out);
		index.write(out);
		type.write(out);
	}

	@Override
	public int hashCode() {
		return key.hashCode() * 163 + index.hashCode() * 100 + type.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof TripleTextPair) {
			TripleTextPair tp = (TripleTextPair) o;
			return key.equals(tp.key) && index.equals(tp.index);
		}
		return false;
	}

	@Override
	public String toString() {
		return key.toString() + "\t" + index.toString() + "\t"
				+ type.toString();
	}

	@Override
	public int compareTo(TripleTextPair tp) {
		int cmp = key.compareTo(tp.key);
		if (cmp != 0) {
			return cmp;
		}
		cmp = index.compareTo(tp.index);
		if (cmp != 0) {
			return cmp;
		}
		return type.compareTo(tp.type);

	}

	// public static class Comparator extends WritableComparator {
	// private static final IntWritable.Comparator INTWRITABLE_COMPARATOR = new
	// IntWritable.Comparator();
	//
	// public Comparator() {
	// super(TextPair.class);
	// }
	//
	// @Override
	// public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
	// {
	// try {
	// int firstL1 = WritableUtils.decodeVIntSize(b1[s1])
	// + readVInt(b1, s1);
	// int firstL2 = WritableUtils.decodeVIntSize(b2[s2])
	// + readVInt(b2, s2);
	// int cmp = INTWRITABLE_COMPARATOR.compare(b1, s1, firstL1, b2,
	// s2, firstL2);
	// if (cmp != 0) {
	// return cmp;
	// }
	// return INTWRITABLE_COMPARATOR.compare(b1, s1 + firstL1, l1
	// - firstL1, b2, s2 + firstL2, l2 - firstL2);
	// } catch (IOException e) {
	// throw new IllegalArgumentException(e);
	// }
	//
	// }
	// }
	//
	// static {
	// WritableComparator.define(TextPair.class, new Comparator());
	// }
	//
	// public static class FirstComparator extends WritableComparator {
	// private static final IntWritable.Comparator INTWRITABLE_COMPARATOR = new
	// IntWritable.Comparator();
	//
	// public FirstComparator() {
	// super(TextPair.class);
	// }
	//
	// @Override
	// public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
	// {
	// try {
	// int firstL1 = WritableUtils.decodeVIntSize(b1[s1])
	// + readVInt(b1, s1);
	// int firstL2 = WritableUtils.decodeVIntSize(b2[s2])
	// + readVInt(b2, s2);
	// return INTWRITABLE_COMPARATOR.compare(b1, s1, firstL1, b2, s2,
	// firstL2);
	// } catch (IOException e) {
	// throw new IllegalArgumentException(e);
	// }
	// }
	//
	// @Override
	// public int compare(WritableComparable a, WritableComparable b) {
	// if (a instanceof TextPair && b instanceof TextPair) {
	// return ((TextPair) a).key.compareTo(((TextPair) b).key) &
	// ((TextPair) a).table_type.compareTo(((TextPair) b).table_type);
	// }
	// return super.compare(a, b);
	// }
	// }

}
