package com.mrp.object;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class DoubleTextPair implements WritableComparable<DoubleTextPair> {
	private IntWritable first;
	private IntWritable second;

	public DoubleTextPair() {
		set(new IntWritable(), new IntWritable());
	}

	public DoubleTextPair(String first, String second) {
		set(new IntWritable(Integer.parseInt(first)),
				new IntWritable(Integer.parseInt(second)));
	}

	public DoubleTextPair(IntWritable first, IntWritable second) {
		set(first, second);
	}

	public void set(IntWritable first, IntWritable second) {
		this.first = first;
		this.second = second;
	}

	public IntWritable getFirst() {
		return first;
	}

	public IntWritable getSecond() {
		return second;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof DoubleTextPair) {
			DoubleTextPair tp = (DoubleTextPair) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}

	@Override
	public String toString() {
		return first.toString() + "\t" + second.toString();
	}

	@Override
	public int compareTo(DoubleTextPair tp) {
		int cmp = first.compareTo(tp.first);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo(tp.second);
	}
	/*
	public static class Comparator extends WritableComparator {
		private static final IntWritable.Comparator INTWRITABLE_COMPARATOR = new IntWritable.Comparator();

		public Comparator() {
			super(TextPair.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1])
						+ readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2])
						+ readVInt(b2, s2);
				int cmp = INTWRITABLE_COMPARATOR.compare(b1, s1, firstL1, b2,
						s2, firstL2);
				if (cmp != 0) {
					return cmp;
				}
				return INTWRITABLE_COMPARATOR.compare(b1, s1 + firstL1, l1
						- firstL1, b2, s2 + firstL2, l2 - firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}

		}
	}

	static {
		WritableComparator.define(TextPair.class, new Comparator());
	}

	public static class FirstComparator extends WritableComparator {
		private static final IntWritable.Comparator INTWRITABLE_COMPARATOR = new IntWritable.Comparator();

		public FirstComparator() {
			super(TextPair.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1])
						+ readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2])
						+ readVInt(b2, s2);
				return INTWRITABLE_COMPARATOR.compare(b1, s1, firstL1, b2, s2,
						firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			if (a instanceof TextPair && b instanceof TextPair) {
				return ((TextPair) a).first.compareTo(((TextPair) b).first);
			}
			return super.compare(a, b);
		}
	}
*/
}
