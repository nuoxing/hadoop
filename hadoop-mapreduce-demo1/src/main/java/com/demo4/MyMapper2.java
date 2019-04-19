package com.demo4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MyMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {

	private IntWritable  le = new IntWritable(1);

	
	//通常情况map的输入内容键是LongWritable类型，为某一行起始位置相对于文件起始位置的偏移量；值是Text类型，为该行的文本内容
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String str = value.toString();
		String ip = str.split(" ")[0].trim();
		System.out.println(ip);
		Text t = new Text(ip);
		context.write(t, le);
	}
	
	
	
	
}
