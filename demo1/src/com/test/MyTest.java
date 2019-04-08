package com.test;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.swy.MyMap;
import com.swy.MyReduce;

public class MyTest {

	public static void main(String[] args) {
		try {
			//���Բ�д ָ��hadoop��װĿ¼
			System.setProperty("hadoop.home.dir", "F:\\hadoop\\hadoop-2.7.3");
			//����·��
	        String dst = "hdfs://172.18.9.126:9000/user/hadoop/input/README.txt";
	        //���·���������ǲ����ڵģ����ļ���Ҳ���С�
	        String dstOut = "hdfs://172.18.9.126:9000/user/hadoop/output";
	        Configuration hadoopConfig = new Configuration();///���������ļ�
	        Job job = new Job(hadoopConfig);//����һ��job,��JobTrackerʹ��
	        
	        //�����Ҫ���jar���У���Ҫ�������
	        //job.setJarByClass(NewMaxTemperature.class);

	        //jobִ����ҵʱ���������ļ���·��
	        FileInputFormat.addInputPath(job, new Path(dst));
	        FileOutputFormat.setOutputPath(job, new Path(dstOut));

	        //ָ���Զ����Mapper��Reducer��Ϊ�����׶ε���������
	        job.setMapperClass(MyMap.class);
	        job.setReducerClass(MyReduce.class);
	        
	        //���������������Key��Value������
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	        
	        //ִ��job��ֱ�����
	        job.waitForCompletion(true);
	        System.out.println("Finished");
		} catch (Exception e) {
			e.printStackTrace();
		}
	
		
	}

	
		
		

}