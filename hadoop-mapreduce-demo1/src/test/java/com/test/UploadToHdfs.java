package com.test;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * ���ݷ�����һ�����ļ��ϴ���hadoop�ķֲ�ʽ�ļ�ϵͳ
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
		//�õ�hadoop�ķֲ�ʽ�ļ�ϵͳ
		FileSystem fs = FileSystem.get(URI.create(hdfs), conf);
		//���ص��ļ������ϴ���h
		fs.copyFromLocalFile(new Path(localFilePath), new Path(hdfsFilePath));
	} catch (IOException e) {
		e.printStackTrace();
	}
	
}
}
