package com.swy;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 数据分析第一步，文件上传到hadoop的分布式文件系统
 * 
 * @author sjakl
 *
 */
public class UploadToHdfs {

	public static void main(String[] args) {
		// System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.5");
		System.out.println("输入文件=" + args[0]);
		String hdfs = "hdfs://172.19.15.11:9000";
		String localFilePath = args[0];
		// String hdfsFilePath =
		// "hdfs://172.18.9.20:9000/user/hadoop/input/access_2013_05_30.log";
		// String hdfsFilePath =
		// "hdfs://172.19.15.11:9000/user3/hadoop/input/aa2.txt";
		System.out.println("输出文件=" + args[1]);
		String hdfsFilePath = args[1];
		Configuration conf = new Configuration();

		try {
			// 得到hadoop的分布式文件系统
			FileSystem fs = FileSystem.get(URI.create(hdfs), conf);
			// 本地的文件数据上传到h
			fs.copyFromLocalFile(new Path(localFilePath), new Path(hdfsFilePath));
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("处理结束");

	}
}
