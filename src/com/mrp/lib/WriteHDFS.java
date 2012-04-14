package com.mrp.lib;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteHDFS {

	private static String GLOBAL_PATH = "MRP/global/";
	private static String NEW_LINE = "\r\n";

	public boolean writeGlobalInfo(String name, String[] data) {
		Configuration conf = new Configuration();
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			Path outFile = new Path(GLOBAL_PATH +  name);
			FSDataOutputStream out = fs.create(outFile, true);
			for (String d : data) {
				out.write(d.getBytes());
				out.write(NEW_LINE.getBytes());
			}
			fs.close();
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean writeGlobalInfo(String name, List<String> data) {
		Configuration conf = new Configuration();
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			Path outFile = new Path(GLOBAL_PATH + name);
			FSDataOutputStream out = fs.create(outFile, true);
			for (String d : data) {
				out.write(d.getBytes());
				out.write(NEW_LINE.getBytes());
			}
			fs.close();
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}
}
