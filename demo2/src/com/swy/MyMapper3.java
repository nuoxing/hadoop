package com.swy;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * ���������־�ļ�������ȡip��ַ
 * @author sjakl
 *
 */
public class MyMapper3 extends Mapper<LongWritable, Text, Text, Text> {

	

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
	 
		  //  System.out.println(value);
			context.write(new Text("r"), value);
	   
	  
	}
	
	
	
	
}
