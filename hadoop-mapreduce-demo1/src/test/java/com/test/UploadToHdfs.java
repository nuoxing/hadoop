package com.test;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 数据分析第一步，文件上传到hadoop的分布式文件系统
 * @author sjakl
 *
 */
public class UploadToHdfs {

	
   public static void main(String[] args) {
	String hdfs = "hdfs://172.18.9.20:9000";
	String localFilePath = "C:/Users/sjakl/Desktop/access_2013_05_30.log";
	String hdfsFilePath = "hdfs://172.18.9.20:9000/user/hadoop/input/access_2013_05_30.log";
	Configuration conf = new Configuration();
	try {
		//得到hadoop的分布式文件系统
		FileSystem fs = FileSystem.get(URI.create(hdfs), conf);
		//本地的文件数据上传到h
		fs.copyFromLocalFile(new Path(localFilePath), new Path(hdfsFilePath));
	} catch (IOException e) {
		e.printStackTrace();
	}
	
}
}
