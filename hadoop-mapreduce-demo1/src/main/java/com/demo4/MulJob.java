package com.demo4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *  MapReduce �� Job ����

	����
	
	һ���Ը��ӵ�Ĵ����߼�������Ҫ��� MapReduce ������������ job �Ĵ������Խ��� MapReduce ��ܵ� JobControl ʵ��
	
	ʵ��
	
	���������� MapReduce ���񣬷ֱ��� Flow �� SumMR �� SortMR��������������ϵ��SumMR ������� SortMR �����룬���� SortMR ���������� SumMR ���֮��
 * @author Administrator
 *
 */
public class MulJob {
	
	
	public static void main(String[] args) throws IOException, InterruptedException {
		Configuration conf1 = new Configuration();
	    Configuration conf2 = new Configuration();
	    
	    Job job1 = Job.getInstance(conf1);
	    Job job2 = Job.getInstance(conf2);
	        
	   // job1.setJarByClass(MRScore3.class);
	    job1.setMapperClass(MyMapper.class);
	   // job1.setReducerClass(MyReduce.class);
	    
	    
	 
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(IntWritable.class);
	  
	    
	    Path inputPath = new Path("F:/hadoop-data/a.txt");
	    Path outputPath = new Path("F:/hadoop-data/output_hw3_1");
	        
	    FileInputFormat.setInputPaths(job1, inputPath);
	    FileOutputFormat.setOutputPath(job1, outputPath);
	    
	    job2.setMapperClass(MyMapper2.class);
	    job2.setReducerClass(MyReduce2.class);
	    
	  
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(IntWritable.class);
	    
	    Path inputPath2 = new Path("F:/hadoop-data/output_hw3_1");
	    Path outputPath2 = new Path("F:/hadoop-data/output_hw3_end");
	        
	    FileInputFormat.setInputPaths(job2, inputPath2);
	    FileOutputFormat.setOutputPath(job2, outputPath2);
	    
	    JobControl control = new JobControl("Score3");
	    
	    ControlledJob aJob = new ControlledJob(job1.getConfiguration());
	    ControlledJob bJob = new ControlledJob(job2.getConfiguration());
	    // ������ҵ������ϵ
	    bJob.addDependingJob(aJob);
	    
	    control.addJob(aJob);
	    control.addJob(bJob);
	    
	    Thread thread = new Thread(control);
	    thread.start();
	    
	    while(!control.allFinished()) {
	        thread.sleep(1000);
	    }
	    System.exit(0);
	}
	
	
}
