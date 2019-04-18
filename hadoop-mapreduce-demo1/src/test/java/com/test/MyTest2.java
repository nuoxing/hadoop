package com.test;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.swy.MyMapper2;
import com.swy.MyReduce2;

/**
 * ͳ�Ʒ��������ĵ�ip�ͷ���ֵ
 * @author sjakl
 *
 */
public class MyTest2 {

	public static void main(String[] args) {
		try {
			//���Բ�д ָ��hadoop��װĿ¼
		    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.5");
			//����·��
	        String dst = "hdfs://192.168.200.156:9000/a.txt";
	        //���·���������ǲ����ڵģ����ļ���Ҳ���С�
	        String dstOut = "hdfs://192.168.200.156:9000/a2";
	        Configuration hadoopConfig = new Configuration();///���������ļ�
	        Job job = Job.getInstance(hadoopConfig);//����һ��job,��JobTrackerʹ��
	        
	        //�����Ҫ���jar���У���Ҫ�������
	        //job.setJarByClass(NewMaxTemperature.class);

	        //jobִ����ҵʱ���������ļ���·��
	        FileInputFormat.addInputPath(job, new Path(dst));
	        FileOutputFormat.setOutputPath(job, new Path(dstOut));

	        //ָ���Զ����Mapper��Reducer��Ϊ�����׶ε���������
	        job.setMapperClass(MyMapper2.class);
	        job.setReducerClass(MyReduce2.class);
	        
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
