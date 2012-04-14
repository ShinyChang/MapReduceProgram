package com.mrp.lib;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CheckAndDelete {
	public static boolean checkAndDelete(final String path, Configuration conf) {
		Path dst_path = new Path(path);
		try {
			FileSystem hdfs = dst_path.getFileSystem(conf);
			if (hdfs.exists(dst_path)) {
				hdfs.delete(dst_path, true);
				return true;
			} else {
				return false;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}
}