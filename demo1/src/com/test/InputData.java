package com.test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

/**
 * ���������ϴ���hadoop��
 * @author sjakl
 *
 */
public class InputData {

	public static void main(String[] args) throws IOException {
		String localsrc = "C:/Users/sjakl/Desktop/access_2013_05_30.log";
		String dst = "hdfs://172.18.9.55:9000/user/hadoop/input/access_2013_05_30.log";
		InputStream in = new BufferedInputStream(new FileInputStream(localsrc));

		Configuration conf = new Configuration();
		//�õ�hadoop�ļ�ϵͳ
		FileSystem fs = FileSystem.get(URI.create(dst), conf);

	    OutputStream out = fs.create(new Path(dst), new Progressable() {
			public void progress() {
				System.out.print(".");// ������ʾ�ļ����ƽ���
			}
		}); 
	    
		IOUtils.copy(in, out);
		

	}
}
