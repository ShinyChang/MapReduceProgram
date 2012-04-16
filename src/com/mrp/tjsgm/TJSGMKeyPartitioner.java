package com.mrp.tjsgm;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import com.mrp.object.QuadTextPair;

public class TJSGMKeyPartitioner extends Partitioner<QuadTextPair, Text> {

	@Override
	public int getPartition(QuadTextPair key, Text value, int numPartitions) {

		// return key.getKey().hashCode() & Integer.MAX_VALUE % numPartitions;
		return (Integer.parseInt(key.getIndex().toString())) % numPartitions;
	}

}
