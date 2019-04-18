package com.test;

import org.apache.hadoop.conf.Configuration;

/**
 *  MapReduce �� Job ����

	����
	
	һ���Ը��ӵ�Ĵ����߼�������Ҫ��� MapReduce ������������ job �Ĵ������Խ��� MapReduce ��ܵ� JobControl ʵ��
	
	ʵ��
	
	���������� MapReduce ���񣬷ֱ��� Flow �� SumMR �� SortMR��������������ϵ��SumMR ������� SortMR �����룬���� SortMR ���������� SumMR ���֮��
 * @author Administrator
 *
 */
public class MulMapTest {
	Configuration conf1 = new Configuration();
    Configuration conf2 = new Configuration();
    
    Job job1 = Job.getInstance(conf1);
    Job job2 = Job.getInstance(conf2);
        
    job1.setJarByClass(MRScore3.class);
    job1.setMapperClass(MRMapper3_1.class);
    //job.setReducerClass(ScoreReducer3.class);
    
    
    job1.setMapOutputKeyClass(IntWritable.class);
    job1.setMapOutputValueClass(StudentBean.class);
    job1.setOutputKeyClass(IntWritable.class);
    job1.setOutputValueClass(StudentBean.class);
    
    job1.setPartitionerClass(CoursePartitioner2.class);
    
    job1.setNumReduceTasks(4);
    
    Path inputPath = new Path("D:\\MR\\hw\\work3\\input");
    Path outputPath = new Path("D:\\MR\\hw\\work3\\output_hw3_1");
        
    FileInputFormat.setInputPaths(job1, inputPath);
    FileOutputFormat.setOutputPath(job1, outputPath);
    
    job2.setMapperClass(MRMapper3_2.class);
    job2.setReducerClass(MRReducer3_2.class);
    
    job2.setMapOutputKeyClass(IntWritable.class);
    job2.setMapOutputValueClass(StudentBean.class);
    job2.setOutputKeyClass(StudentBean.class);
    job2.setOutputValueClass(NullWritable.class);
    
    Path inputPath2 = new Path("D:\\MR\\hw\\work3\\output_hw3_1");
    Path outputPath2 = new Path("D:\\MR\\hw\\work3\\output_hw3_end");
        
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
