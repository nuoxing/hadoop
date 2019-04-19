package com.demo4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MyMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {

	private IntWritable  le = new IntWritable(1);

	
	//ͨ�����map���������ݼ���LongWritable���ͣ�Ϊĳһ����ʼλ��������ļ���ʼλ�õ�ƫ������ֵ��Text���ͣ�Ϊ���е��ı�����
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
