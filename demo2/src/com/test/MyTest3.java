package com.test;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.swy.MyMapper3;
import com.swy.MyReduce3;

/**
 * ͳ�Ʒ��������ĵ�ip�ͷ���ֵ
 * @author sjakl
 *
 */
public class MyTest3 {

	public static void main(String[] args) {
		try {
			//���Բ�д ָ��hadoop��װĿ¼
		    //System.setProperty("hadoop.home.dir", "F:\\hadoop\\hadoop-2.7.3");
			//����·��
	        String dst = "hdfs://172.18.9.20:9000/user/hadoop/output/access_res/part-r-00000";
	        //���·���������ǲ����ڵģ����ļ���Ҳ���С�
	        String dstOut = "hdfs://172.18.9.20:9000/user/hadoop/output/access_res3";
	        Configuration hadoopConfig = new Configuration();///���������ļ�
	        Job job = new Job(hadoopConfig);//����һ��job,��JobTrackerʹ��
	        
	        //�����Ҫ���jar���У���Ҫ�������
	        //job.setJarByClass(NewMaxTemperature.class);

	        //jobִ����ҵʱ���������ļ���·��
	        FileInputFormat.addInputPath(job, new Path(dst));
	        FileOutputFormat.setOutputPath(job, new Path(dstOut));

	        //ָ���Զ����Mapper��Reducer��Ϊ�����׶ε���������
	        job.setMapperClass(MyMapper3.class);
	        job.setReducerClass(MyReduce3.class);
	        
	        //����map������������Key��Value������
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        
	        //ִ��job��ֱ�����
	        job.waitForCompletion(true);
	        System.out.println("Finished");
		} catch (Exception e) {
			e.printStackTrace();
		}
	
		
	}

	
		
		

}
