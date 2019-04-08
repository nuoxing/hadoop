package com.swy;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 把输入的日志文件处理，获取ip地址
 * @author sjakl
 *
 */
public class MyMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {

	

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		IntWritable i = null ;
		Text t = null;
		//System.out.println(value);
		  StringTokenizer itr = new StringTokenizer(value.toString());
	        if (itr.hasMoreTokens()) {
	        	 t = new Text(itr.nextToken());
	        }
	        
	        if (itr.hasMoreTokens()) {
	        	 i = new IntWritable(Integer.valueOf(itr.nextToken()));
	        }
	        
	
		context.write(t, i);
	}
	
	
	
	
}
