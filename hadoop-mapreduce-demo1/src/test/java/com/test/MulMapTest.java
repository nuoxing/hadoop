package com.test;

import org.apache.hadoop.conf.Configuration;

/**
 *  MapReduce 多 Job 串联

	需求
	
	一个稍复杂点的处理逻辑往往需要多个 MapReduce 程序串联处理，多 job 的串联可以借助 MapReduce 框架的 JobControl 实现
	
	实例
	
	以下有两个 MapReduce 任务，分别是 Flow 的 SumMR 和 SortMR，其中有依赖关系：SumMR 的输出是 SortMR 的输入，所以 SortMR 的启动得在 SumMR 完成之后
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
    // 设置作业依赖关系
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
