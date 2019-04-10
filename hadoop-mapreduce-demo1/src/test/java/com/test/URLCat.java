package com.test;

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

/**
 * ��ȡhadoop�ϵ��ļ�����
 * @author sjakl
 *
 */
public class URLCat {

	  static{
	        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	    }
	    public static void main(String[] args) throws Exception {
	    	//���Բ�д ָ��hadoop��װĿ¼
			System.setProperty("hadoop.home.dir", "F:\\hadoop\\hadoop-2.7.3");
	        InputStream in = null;
	        try{
	            in = new URL("hdfs://172.18.9.126:9000/user/hadoop/input/README.txt").openStream();
	            IOUtils.copyBytes(in, System.out, 4096, false);
	        } finally{
	            IOUtils.closeStream(in);
	        }
	    }
}
